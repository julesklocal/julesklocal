/**
 * JuleskLocal — Auto Data Pipeline
 * Vercel Cron Job: runs every 6 hours
 * Pulls from: 211.org, Palm Coast Open Data, NPPES (doctors), Eventbrite
 * Stores into: Supabase (community_posts table)
 *
 * Deploy path: /api/cron/fetch-community-data.js
 * vercel.json schedule: "0 */6 * * *"
 */

import { createClient } from '@supabase/supabase-js';

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY
);

// ─── CONFIG ───────────────────────────────────────────────
const PALM_COAST_ZIP = '32164';

// 211 API — sign up free at apiportal.211.org
const API_211_KEY = process.env.API_211_KEY || null;

// NPPES — no key needed, fully public
const NPPES_BASE = 'https://npiregistry.cms.hhs.gov/api';

// Palm Coast open data — public ArcGIS, no key needed
const PALM_COAST_OPEN_DATA = 'https://opendata.palmcoast.gov/datasets';

// ─── MAIN HANDLER ─────────────────────────────────────────
export default async function handler(req, res) {
  // Security: only allow Vercel cron or manual trigger with secret
  const authHeader = req.headers.authorization;
  if (
    req.headers['x-vercel-cron'] !== '1' &&
    authHeader !== `Bearer ${process.env.CRON_SECRET}`
  ) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  console.log('[JuleskLocal Cron] Starting data fetch...');
  const results = { success: [], failed: [] };

  // Run all fetchers in parallel
  const fetches = await Promise.allSettled([
    fetch211Resources(PALM_COAST_ZIP),
    fetchNPPESDoctors(PALM_COAST_ZIP),
    fetchPalmCoastPermits(),
    fetchFLCommunityEvents(PALM_COAST_ZIP),
  ]);

  for (const fetch of fetches) {
    if (fetch.status === 'fulfilled') {
      results.success.push(fetch.value);
    } else {
      results.failed.push(fetch.reason?.message || 'Unknown error');
      console.error('[Cron Error]', fetch.reason);
    }
  }

  console.log('[JuleskLocal Cron] Done.', results);
  return res.status(200).json({
    message: 'Data sync complete',
    timestamp: new Date().toISOString(),
    results,
  });
}

// ─── 211 COMMUNITY RESOURCES ──────────────────────────────
async function fetch211Resources(zip) {
  // 211 National Data Platform Search API
  // Docs: https://apiportal.211.org
  // Free trial key available — sign up at apiportal.211.org
  // Without a key we fall back to the public Open211 format

  let resources = [];

  if (API_211_KEY) {
    const url = `https://api.211.org/search/v1/api/Search?keyword=community+resources&location=${zip}&distance=10&size=20`;
    const resp = await fetch(url, {
      headers: { 'Ocp-Apim-Subscription-Key': API_211_KEY }
    });
    const data = await resp.json();
    resources = (data.records || []).map(r => ({
      type: 'resource',
      title: r.ProgramName || r.AgencyName,
      body: r.ProgramDescription || r.AgencyDescription || '',
      zip,
      source: '211',
      source_id: r.ResourceAgencyNum || r.id,
      phone: r.Phone || null,
      address: r.PhysicalAddress || null,
      url: r.URL || null,
      is_free: true,
      raw: r,
    }));
  } else {
    // Fallback: Florida 211 public resource directory
    // navigateresources.net covers Central Florida including Palm Coast
    console.log('[211] No API key — using fallback public 211 data');
    resources = getFallback211Resources(zip);
  }

  return await upsertPosts(resources, '211');
}

// ─── NPPES DOCTOR LOOKUP ──────────────────────────────────
async function fetchNPPESDoctors(zip) {
  // NPPES NPI Registry — 100% free, no API key needed
  // Returns every licensed healthcare provider in a zip
  const url = `${NPPES_BASE}/?version=2.1&postal_code=${zip}&limit=50&skip=0&enumeration_type=NPI-1`;

  const resp = await fetch(url);
  const data = await resp.json();

  const doctors = (data.results || []).map(d => {
    const basic = d.basic || {};
    const addr = (d.addresses || []).find(a => a.address_purpose === 'LOCATION') || {};
    const taxonomy = (d.taxonomies || []).find(t => t.primary) || {};

    return {
      type: 'provider',
      subtype: 'doctor',
      title: `${basic.first_name || ''} ${basic.last_name || ''}, ${basic.credential || ''}`.trim(),
      body: taxonomy.desc || 'Healthcare Provider',
      specialty: taxonomy.desc || null,
      zip: addr.postal_code?.slice(0, 5) || zip,
      source: 'nppes',
      source_id: d.number,
      phone: addr.telephone_number || null,
      address: `${addr.address_1 || ''} ${addr.city || ''} ${addr.state || ''}`.trim(),
      license_verified: true,
      npi: d.number,
      raw: { basic, taxonomy, addr },
    };
  });

  return await upsertPosts(doctors, 'nppes');
}

// ─── PALM COAST BUILDING PERMITS ─────────────────────────
async function fetchPalmCoastPermits() {
  // Palm Coast Open Data Hub — ArcGIS REST API, no key needed
  // opendata.palmcoast.gov
  // Building permits = service requests waiting to happen
  const url = `https://services.arcgis.com/LZWBMFTqpKoZiMOq/arcgis/rest/services/Building_Permits/FeatureServer/0/query?where=1%3D1&outFields=*&resultRecordCount=25&orderByFields=IssueDate+DESC&f=json`;

  let permits = [];
  try {
    const resp = await fetch(url);
    const data = await resp.json();
    permits = (data.features || []).map(f => {
      const p = f.attributes || {};
      return {
        type: 'update',
        subtype: 'permit',
        title: `Building Permit: ${p.WorkDescription || p.PermitType || 'New Permit Issued'}`,
        body: `${p.PermitType || ''} — ${p.WorkDescription || ''}. Issued: ${p.IssueDate ? new Date(p.IssueDate).toLocaleDateString() : 'Recent'}`.trim(),
        zip: PALM_COAST_ZIP,
        source: 'palm_coast_open_data',
        source_id: String(p.PermitNumber || p.OBJECTID),
        address: p.SiteAddress || null,
        raw: p,
      };
    });
  } catch (e) {
    // ArcGIS endpoint may vary — fall back gracefully
    console.log('[Permits] ArcGIS endpoint not available, skipping');
    permits = [];
  }

  return await upsertPosts(permits, 'palm_coast_permits');
}

// ─── COMMUNITY EVENTS (Eventbrite public) ─────────────────
async function fetchFLCommunityEvents(zip) {
  // Eventbrite public search — free, no key for basic search
  // Falls back gracefully if rate limited
  const url = `https://www.eventbriteapi.com/v3/events/search/?location.address=${zip}&location.within=10mi&categories=113,115&expand=venue&token=${process.env.EVENTBRITE_KEY || ''}`;

  let events = [];
  if (!process.env.EVENTBRITE_KEY) {
    console.log('[Events] No Eventbrite key, skipping');
    return { source: 'eventbrite', inserted: 0, skipped: 0 };
  }

  try {
    const resp = await fetch(url);
    const data = await resp.json();
    events = (data.events || []).map(e => ({
      type: 'update',
      subtype: 'event',
      title: e.name?.text || 'Community Event',
      body: e.description?.text?.slice(0, 300) || '',
      zip,
      source: 'eventbrite',
      source_id: e.id,
      url: e.url,
      address: e.venue?.address?.localized_address_display || null,
      starts_at: e.start?.utc || null,
      is_free: e.is_free,
      raw: e,
    }));
  } catch (e) {
    console.log('[Events] Eventbrite fetch failed:', e.message);
  }

  return await upsertPosts(events, 'eventbrite');
}

// ─── FALLBACK 211 DATA (no API key) ──────────────────────
function getFallback211Resources(zip) {
  // Static seed data for Palm Coast / Flagler County
  // Replace with real 211 API once key is obtained
  return [
    {
      type: 'resource',
      title: 'Flagler County Health Department',
      body: 'Public health services, immunizations, WIC, family planning, and disease prevention for Flagler County residents.',
      zip,
      source: '211_seed',
      source_id: 'flagler-health-dept',
      phone: '386-437-7350',
      address: '301 Dr. Carter Blvd, Bunnell, FL 32110',
      url: 'https://www.floridahealth.gov/counties/flagler',
      is_free: true,
      raw: {},
    },
    {
      type: 'resource',
      title: 'Palm Coast Food Pantry',
      body: 'Emergency food assistance for residents of Palm Coast and Flagler County. No income verification required for first visit.',
      zip,
      source: '211_seed',
      source_id: 'palm-coast-food-pantry',
      phone: '386-446-6333',
      address: 'Palm Coast, FL 32164',
      is_free: true,
      raw: {},
    },
    {
      type: 'resource',
      title: 'Flagler County Community Services',
      body: 'Emergency assistance, utility help, housing support, and referral services for Flagler County households in need.',
      zip,
      source: '211_seed',
      source_id: 'flagler-community-services',
      phone: '386-313-4100',
      address: 'Flagler County, FL',
      url: 'https://www.flaglercounty.gov',
      is_free: true,
      raw: {},
    },
  ];
}

// ─── SUPABASE UPSERT ──────────────────────────────────────
async function upsertPosts(posts, source) {
  if (!posts.length) return { source, inserted: 0, skipped: 0 };

  let inserted = 0;
  let skipped = 0;

  for (const post of posts) {
    const record = {
      type: post.type,
      subtype: post.subtype || null,
      title: post.title,
      body: post.body,
      zip: post.zip,
      source: post.source,
      source_id: post.source_id,
      phone: post.phone || null,
      address: post.address || null,
      url: post.url || null,
      is_free: post.is_free || false,
      license_verified: post.license_verified || false,
      specialty: post.specialty || null,
      npi: post.npi || null,
      starts_at: post.starts_at || null,
      raw_data: post.raw || {},
      last_synced_at: new Date().toISOString(),
    };

    const { error } = await supabase
      .from('community_posts')
      .upsert(record, {
        onConflict: 'source,source_id',
        ignoreDuplicates: false, // update if changed
      });

    if (error) {
      console.error(`[Supabase] Upsert error for ${post.source_id}:`, error.message);
      skipped++;
    } else {
      inserted++;
    }
  }

  console.log(`[${source}] ${inserted} upserted, ${skipped} failed`);
  return { source, inserted, skipped };
}
