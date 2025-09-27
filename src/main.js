// src/main.js — Indeed scraper (HTTP + Cheerio only; Crawlee v3-safe)
// - RSS bootstrap for discovery (gives posted date via <pubDate>)
// - Mobile SERP fallback if RSS empty
// - Detail fetches only /viewjob?jk=... (mobile-first, desktop inline fallback if thin)
// - Anti-block: conservative concurrency, pacing, session rotation, never touches /rc/clk or /pagead/clk
// - Outputs: title, company, location, description_html, description_text, date_posted, jobpasted (alias), job_types, url

import { Actor } from 'apify';
import {
  CheerioCrawler,
  Dataset,
  log,
  sleep,
  requestAsBrowser, // HTTP fetch (used for RSS bootstrap)
} from 'crawlee';
import * as cheerio from 'cheerio';

// ----------------- CONSTANTS -----------------
const DESKTOP = 'https://www.indeed.com';
const MOBILE = 'https://m.indeed.com';
const DATE_MAP = { '24h': '1', '7d': '7', '30d': '30' };

// Soft global rate limit across all requests (helps with 403/590 churn)
const MIN_GAP_MS = 1100;
let lastHit = 0;
async function pace() {
  const now = Date.now();
  const delta = now - lastHit;
  if (delta < MIN_GAP_MS) await sleep(MIN_GAP_MS - delta);
  lastHit = Date.now();
}

const norm = (t) => (t || '').replace(/\s+/g, ' ').trim();
const rmNoise = ($root) => { $root('script,style,noscript').remove(); };

// Modest UA pool (desktop & mobile)
const UAS_DESKTOP = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
];
const UAS_MOBILE = [
  'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1',
  'Mozilla/5.0 (Linux; Android 14; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36',
];
const pickUA = (mobile = false) => (mobile ? UAS_MOBILE : UAS_DESKTOP)[Math.floor(Math.random() * (mobile ? UAS_MOBILE.length : UAS_DESKTOP.length))];

const jkFrom = (href) => {
  try { const u = new URL(href, DESKTOP); return u.searchParams.get('jk'); } catch { return null; }
};

const getText = ($, sels) => {
  for (const s of sels) {
    const el = $(s).first();
    if (el.length) {
      const txt = norm(el.text());
      if (txt) return txt;
    }
  }
  return null;
};
const getHtml = ($, sels) => {
  for (const s of sels) {
    const el = $(s).first();
    if (el.length) {
      const html = el.html();
      if (html && norm(html)) return html.trim();
    }
  }
  return null;
};

const postedFromDetail = ($) => {
  const t = norm($('div.jobsearch-JobMetadataFooter, [data-testid="jobsearch-JobMetadataFooter"]').text());
  if (!t) return null;
  for (const re of [
    /(Just posted|Today)/i,
    /Posted\s+\d+\+?\s+(?:day|days|hour|hours)\s+ago/i,
    /\d+\+?\s+(?:day|days|hour|hours)\s+ago/i,
    /Active\s+\d+\+?\s+(?:day|days|hour|hours)\s+ago/i,
  ]) {
    const m = t.match(re);
    if (m) return m[0];
  }
  const m2 = t.match(/Posted[^|]+/i);
  return m2 ? m2[0].trim() : null;
};

function extractDetail($, url, seed) {
  rmNoise($);

  const title = getText($, [
    'h1[data-testid="jobsearch-JobTitle"]',
    'h1.jobsearch-JobInfoHeader-title',
    'h1',
    'h1.jobsearch-JobInfoHeader-title-container',
  ]) ?? seed?.title ?? null;

  const company = getText($, [
    '[data-company-name] a',
    '[data-company-name]',
    'div[data-testid="inlineHeader-companyName"]',
    '.jobsearch-CompanyInfoWithoutHeaderImage div a',
    '.jobsearch-CompanyInfoWithoutHeaderImage div',
    'div.jobsearch-CompanyInfoContainer a',
    'div.jobsearch-CompanyInfoContainer',
  ]) ?? seed?.company ?? null;

  const location = getText($, [
    'div[data-testid="inlineHeader-companyLocation"]',
    '.jobsearch-CompanyInfoWithoutHeaderImage > div:last-child',
    'div.jobsearch-CompanyInfoContainer ~ div',
  ]) ?? seed?.location ?? null;

  const description_html = getHtml($, [
    '#jobDescriptionText',
    'div#jobDescriptionText',
    'section#jobDescriptionText',
    '#jobDescriptionTextContainer',
    'div#jobDescriptionTextContainer',
  ]);

  let description_text = null;
  if (description_html) {
    const $$ = cheerio.load(description_html);
    $$('script,style,noscript').remove();
    description_text = norm($$.text()) || null;
  } else {
    description_text = norm($('#jobDescriptionText, section#jobDescriptionText, article, #jobDescriptionTextContainer').text()) || null;
  }

  const date_posted = postedFromDetail($) ?? seed?.date_posted ?? null;

  const job_types = (() => {
    const out = new Set();
    $('div[data-testid="job-details"] div:contains("Job type")').next().find('li').each((_, li) => out.add(norm($(li).text())));
    $('li').each((_, li) => {
      const t = norm($(li).text());
      if (/full[-\s]?time|part[-\s]?time|contract|temporary|intern(ship)?|commission|per[-\s]?diem|apprenticeship|remote/i.test(t)) out.add(t);
    });
    const arr = Array.from(out).filter(Boolean);
    return arr.length ? arr : null;
  })();

  return {
    title,
    company,
    location,
    date_posted,
    jobpasted: date_posted, // alias for compatibility with your expected column
    description_html: description_html || null,
    description_text,
    job_types,
    url,
  };
}

// ----------------- MAIN -----------------
await Actor.init();

const input = (await Actor.getInput()) || {};
const {
  keyword = 'office',
  location = 'United States',
  postedWithin = '7d',        // '24h' | '7d' | '30d'
  results_wanted = 100,
  maxConcurrency = 2,         // start conservative; scale once stable
  proxyConfiguration = null,
} = input;

const proxyConfig = await Actor.createProxyConfiguration(proxyConfiguration);

// ---------- 1) RSS bootstrap (cheap; provides posted date via <pubDate>) ----------
const fromage = DATE_MAP[postedWithin] || '7';
const rssUrl = new URL('/rss', DESKTOP);
if (keyword) rssUrl.searchParams.set('q', keyword);
if (location) rssUrl.searchParams.set('l', location);
rssUrl.searchParams.set('fromage', fromage);

let seedDetails = [];
try {
  for (let i = 0; i < 5 && seedDetails.length === 0; i++) {
    const px = proxyConfig ? await proxyConfig.newUrl() : undefined;
    await pace();
    const resp = await requestAsBrowser({
      url: rssUrl.href,
      proxyUrl: px,
      headers: {
        'User-Agent': pickUA(false),
        'Accept': 'application/rss+xml, application/xml;q=0.9, */*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache',
        'Connection': 'close',
      },
      http2: false,
      timeout: { request: 45000 },
    });

    if ((resp.statusCode || 0) >= 500 || resp.statusCode === 429) {
      log.warning(`RSS HTTP ${resp.statusCode}; retrying…`);
      continue;
    }
    if ((resp.statusCode || 0) >= 400) {
      log.warning(`RSS HTTP ${resp.statusCode}; will fall back to SERP.`);
      break;
    }

    const $x = cheerio.load(resp.body || '', { xmlMode: true });
    $x('item').each((_, el) => {
      if (seedDetails.length >= results_wanted) return;
      const $el = $x(el);
      const link = $el.find('link').text();
      const title = norm($el.find('title').text());
      const pubDate = norm($el.find('pubDate').text());
      const jk = jkFrom(link);
      if (!jk) return;

      // title often like "Role - Company - Location"
      const parts = title.split(' - ').map(norm).filter(Boolean);
      const seed = {
        label: 'DETAIL',
        title: parts[0] || title || null,
        company: parts.length >= 2 ? parts[parts.length - 2] : null,
        location: parts.length >= 3 ? parts[parts.length - 1] : null,
        date_posted: pubDate || null,
      };
      seedDetails.push({ url: `${MOBILE}/viewjob?jk=${jk}`, userData: seed });
    });
  }

  if (seedDetails.length) log.info(`Bootstrapped ${seedDetails.length} jobs from RSS.`);
} catch (e) {
  log.warning(`RSS bootstrap failed: ${e.message}`);
}

// ---------- 2) Mobile SERP discovery if RSS produced nothing ----------
const listSeedUrl = (() => {
  const u = new URL('/jobs', MOBILE);
  if (keyword) u.searchParams.set('q', keyword);
  if (location) u.searchParams.set('l', location);
  u.searchParams.set('fromage', fromage);
  return u.href;
})();

const state = { saved: 0, enq: 0 };

// ---------- CheerioCrawler (Crawlee v3-safe) ----------
const crawler = new CheerioCrawler({
  proxyConfiguration: proxyConfig,
  maxConcurrency: Math.max(1, Number(maxConcurrency) || 2),
  useSessionPool: true,
  persistCookiesPerSession: true,
  maxRequestRetries: 8,
  requestHandlerTimeoutSecs: 75,
  navigationTimeoutSecs: 75,
  sessionPoolOptions: {
    maxPoolSize: Math.max(30, maxConcurrency * 12),
    sessionOptions: { maxUsageCount: 20 },
  },

  preNavigationHooks: [
    async ({ request, session }, gotOptions) => {
      const isMobile = request.url.startsWith(MOBILE);
      const ua = session?.userData?.ua || pickUA(isMobile);
      if (session && !session.userData.ua) session.userData.ua = ua;

      // conservative headers; keep HTTP/1.1 semantics via Connection: close
      gotOptions.headers = {
        ...(gotOptions.headers || {}),
        'User-Agent': ua,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache',
        'Upgrade-Insecure-Requests': '1',
        'Referer': 'https://www.google.com/',
        'Connection': 'close',
      };

      // backoff based on retryCount + global pacing
      const backoff = Math.min(5000, 700 * (1 << request.retryCount)); // 0,700,1400,2800,5600
      if (backoff > 0) await sleep(backoff);
      await pace();
    },
  ],

  async requestHandler(ctx) {
    const { request, $, addRequests, sendRequest, response, session, log } = ctx;
    const status = response?.statusCode ?? response?.status;

    // Mark bad sessions on hard blocks to encourage rotation
    if (status === 403 || status === 429) {
      session?.markBad();
      throw new Error(`Blocked ${status}`);
    }
    if (status >= 500) {
      session?.markBad();
      throw new Error(`HTTP ${status}`);
    }

    const { label } = request.userData || {};

    // Mobile SERP listing
    if (label === 'LIST') {
      if (state.saved >= results_wanted) return;

      const detailReqs = [];
      $('a[href*="/viewjob?jk="]').each((_, el) => {
        if (state.enq >= results_wanted) return;
        const href = $(el).attr('href');
        const jk = jkFrom(href);
        if (!jk) return;

        const url = `${MOBILE}/viewjob?jk=${jk}`;
        const $card = $(el).closest('li, div');
        const seed = { label: 'DETAIL' };

        const sTitle = norm($card.find('h2, h3, .jobTitle').first().text());
        const sCompany = norm($card.find('[data-company-name], .companyName').first().text());
        const sLoc = norm($card.find('[data-testid="text-location"], .companyLocation').first().text());
        const sPosted = norm(
          $card.find('span:contains("Posted"), span:contains("Just posted"), span:contains("Today"), span:contains("Active"), span.date, .css-1v0q9r6').first().text()
        );

        if (sTitle) seed.title = sTitle;
        if (sCompany) seed.company = sCompany;
        if (sLoc) seed.location = sLoc;
        if (sPosted) seed.date_posted = sPosted;

        detailReqs.push({ url, userData: seed });
        state.enq += 1;
      });

      if (detailReqs.length) await addRequests(detailReqs);

      // paginate only if we still need more
      if (state.enq < results_wanted) {
        let nextUrl;
        const nextHref = $('a[aria-label="Next"], a[data-testid="pagination-page-next"]').attr('href');
        if (nextHref) {
          nextUrl = new URL(nextHref, MOBILE).href;
        } else {
          const u = new URL(request.url);
          const start = parseInt(u.searchParams.get('start') || '0', 10);
          u.searchParams.set('start', String(start + 10));
          nextUrl = u.href;
        }
        await addRequests([{ url: nextUrl, userData: { label: 'LIST' } }]);
      }
      return;
    }

    // DETAIL: mobile-first; inline desktop fallback if content looks thin
    let $$ = $;
    const thin = !$$ || (!$('#jobDescriptionText, #jobDescriptionTextContainer').length && $$.root().text().trim().length < 80);

    if (thin) {
      try {
        const jk = jkFrom(request.url) || request.url.match(/[?&]jk=([a-z0-9]+)/i)?.[1];
        if (jk) {
          await pace();
          const resp = await sendRequest({ url: `${DESKTOP}/viewjob?jk=${jk}`, responseType: 'text' });
          if (resp.statusCode === 200 && resp.body?.length > 100) {
            $$ = cheerio.load(resp.body);
          }
        }
      } catch {
        // keep seed-only if fallback blocked
      }
    }

    if (!$$) $$ = cheerio.load('<html></html>');
    const result = extractDetail($$, request.url, request.userData);

    await Dataset.pushData(result);
    state.saved += 1;
  },

  failedRequestHandler({ request, error, session, log }) {
    session?.markBad?.();
    log.error(`Failed after retries: ${request.url} — ${error?.message}`);
  },
});

// ---------- Kickoff ----------
if (seedDetails.length) {
  await crawler.addRequests(seedDetails.slice(0, results_wanted));
  log.info(`Discovery via RSS: enqueued ${Math.min(seedDetails.length, results_wanted)} detail pages.`);
} else {
  await crawler.addRequests([{ url: listSeedUrl, userData: { label: 'LIST' } }]);
  log.info(`Discovery via mobile SERP: ${listSeedUrl}`);
}

await crawler.run();
log.info(`Done. Saved ${state.saved} jobs.`);
await Actor.exit();
