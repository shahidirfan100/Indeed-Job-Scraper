// Indeed jobs scraper — HTTP requests + Cheerio (no Crawlee request helpers).
// Discovery: Mobile SERP -> Desktop SERP -> RSS (as last resort)
// Each SERP page uses 3 extraction modes: DOM anchors, Mosaic JSON, and Regex.
// Detail: Mobile /viewjob?jk=… first; Desktop /viewjob?jk=… fallback if mobile thin.
// Anti-blocking: Apify proxy rotation, got-scraping header generator, pacing, backoff.
// Outputs: title, company, location, description_html, description_text, date_posted, job_types, url.

import { Actor, log } from 'apify';
import { gotScraping } from 'got-scraping';
import * as cheerio from 'cheerio';

// ------------ constants ------------
const DATE_MAP = { '24h': '1', '7d': '7', '30d': '30' };
const MIN_GAP_MS = 1100;

let lastTs = 0;
async function pace() {
    const now = Date.now();
    const delta = now - lastTs;
    if (delta < MIN_GAP_MS) await Actor.sleep(MIN_GAP_MS - delta);
    lastTs = Date.now();
}

const norm = (t) => (t || '').replace(/\s+/g, ' ').trim();
const stripDom = ($root) => { $root('script,style,noscript').remove(); };

const jkFrom = (href, base) => {
    try { const u = new URL(href, base); return u.searchParams.get('jk'); } catch { return null; }
};

function buildHeaders({ mobile }) {
    // got-scraping can synthesize realistic headers for us
    return {
        // These two enable header generator
        'sec-fetch-site': 'same-origin',
        'sec-fetch-mode': 'navigate',
        // we'll still set U-A via headerGeneratorOptions; no static UA here
    };
}

async function fetchHtml(url, { proxyConfig, mobile=false, attempts=4, timeoutMs=60000 }) {
    let lastErr;
    for (let i = 0; i < attempts; i++) {
        try {
            await pace();
            const proxyUrl = proxyConfig ? await proxyConfig.newUrl() : undefined;
            const res = await gotScraping({
                url,
                headers: buildHeaders({ mobile }),
                // IMPORTANT: header generator here is OK (the earlier error was only with CheerioCrawler options)
                headerGeneratorOptions: {
                    browsers: [{ name: mobile ? 'chrome' : 'chrome', minVersion: 118 }],
                    devices: [mobile ? 'mobile' : 'desktop'],
                    operatingSystems: mobile ? ['android', 'ios'] : ['windows', 'macos'],
                },
                proxyUrl,
                timeout: { request: timeoutMs },
                http2: false,
                throwHttpErrors: false,
            });

            const sc = res.statusCode || 0;
            if (sc === 403 || sc === 429) { lastErr = new Error(`HTTP ${sc}`); continue; }
            if (sc >= 500) { lastErr = new Error(`HTTP ${sc}`); continue; }
            if (!res.body || res.body.length < 80) { lastErr = new Error(`Thin body (${res.body?.length || 0})`); continue; }
            return res.body;
        } catch (e) {
            lastErr = e;
        }
        const backoff = Math.min(7000, 700 * (1 << i)) + Math.floor(Math.random() * 250);
        await Actor.sleep(backoff);
    }
    throw lastErr || new Error('Failed to fetch');
}

// --- posted date from footer text only ---
function postedFromDetail($) {
    const t = norm($('div.jobsearch-JobMetadataFooter, [data-testid="jobsearch-JobMetadataFooter"]').text());
    if (!t) return null;
    const pats = [
        /(Just posted|Today)/i,
        /Posted\s+\d+\+?\s+(?:day|days|hour|hours)\s+ago/i,
        /\d+\+?\s+(?:day|days|hour|hours)\s+ago/i,
        /Active\s+\d+\+?\s+(?:day|days|hour|hours)\s+ago/i,
    ];
    for (const re of pats) { const m = t.match(re); if (m) return m[0]; }
    const m2 = t.match(/Posted[^|]+/i);
    return m2 ? m2[0].trim() : null;
}

const firstText = ($, sels) => {
    for (const s of sels) {
        const el = $(s).first();
        if (el.length) { const txt = norm(el.text()); if (txt) return txt; }
    }
    return null;
};
const firstHtml = ($, sels) => {
    for (const s of sels) {
        const el = $(s).first();
        if (el.length) {
            const html = el.html();
            if (html && norm(html)) return html.trim();
        }
    }
    return null;
};

function extractDetail($, url, seed) {
    stripDom($);

    const title = firstText($, [
        'h1[data-testid="jobsearch-JobTitle"]',
        'h1.jobsearch-JobInfoHeader-title',
        'h1.jobsearch-JobInfoHeader-title-container',
        'h1',
    ]) ?? seed?.title ?? null;

    const company = firstText($, [
        '[data-company-name] a',
        '[data-company-name]',
        'div[data-testid="inlineHeader-companyName"]',
        '.jobsearch-CompanyInfoWithoutHeaderImage div a',
        '.jobsearch-CompanyInfoWithoutHeaderImage div',
        'div.jobsearch-CompanyInfoContainer a',
        'div.jobsearch-CompanyInfoContainer',
    ]) ?? seed?.company ?? null;

    const location = firstText($, [
        'div[data-testid="inlineHeader-companyLocation"]',
        '.jobsearch-CompanyInfoWithoutHeaderImage > div:last-child',
        'div.jobsearch-CompanyInfoContainer ~ div',
    ]) ?? seed?.location ?? null;

    const description_html = firstHtml($, [
        '#jobDescriptionText',
        'div#jobDescriptionText',
        'section#jobDescriptionText',
        'div#jobDescriptionTextContainer',
        '#jobDescriptionTextContainer',
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
        const types = new Set();
        $('div[data-testid="job-details"] div:contains("Job type")').next().find('li')
            .each((_, li) => types.add(norm($(li).text())));
        $('li').each((_, li) => {
            const t = norm($(li).text());
            if (/full[-\s]?time|part[-\s]?time|contract|temporary|intern(ship)?|commission|per[-\s]?diem|apprenticeship|remote/i.test(t))
                types.add(t);
        });
        const arr = Array.from(types).filter(Boolean);
        return arr.length ? arr : null;
    })();

    return { title, company, location, description_html: description_html || null, description_text, date_posted, job_types, url };
}

// ---- helpers: extract JKs from a SERP html by 3 strategies
function discoverJKsFromDom($, baseUrl) {
    const set = new Set();
    $('a[href*="/viewjob?jk="]').each((_, a) => {
        const href = $(a).attr('href');
        const jk = jkFrom(href, baseUrl);
        if (jk) set.add(jk);
    });
    return Array.from(set);
}

function discoverJKsFromMosaic(html) {
    // Look for window.mosaic.providerData JSON blocks
    const set = new Set();
    const scriptMatches = html.match(/window\.mosaic\.providerData\s*=\s*({[\s\S]*?});\s*<\/script>/i)
        || html.match(/"mosaic-provider-jobcards"[\s\S]*?({[\s\S]*?})\s*<\/script>/i);
    if (scriptMatches) {
        try {
            // Try to parse loosely: extract all jk's from that JSON with regex (safer than full JSON.parse on partial)
            const re = /"jk"\s*:\s*"([a-z0-9]+)"/gi;
            let m;
            while ((m = re.exec(scriptMatches[0])) !== null) set.add(m[1]);
            // Also jobKeysWithInfo arrays
            const re2 = /jobKeysWithInfo[^[]*\[([^\]]+)\]/gi;
            let m2;
            while ((m2 = re2.exec(scriptMatches[0])) !== null) {
                const arr = m2[1].match(/"([a-z0-9]{16})"/gi) || [];
                for (const x of arr) set.add(x.replace(/"/g, ''));
            }
        } catch { /* ignore */ }
    }
    return Array.from(set);
}

function discoverJKsByRegex(html) {
    const set = new Set();
    const re = /\/viewjob\?[^"'<>]*?\bjk=([a-z0-9]{16})/gi;
    let m;
    while ((m = re.exec(html)) !== null) set.add(m[1]);
    return Array.from(set);
}

function uniqueDetailTargets(jks, mobileBase, desktopBase) {
    // Prefer mobile detail pages; desktop only for fallback during detail fetch
    return jks.map((jk) => ({ url: `${mobileBase}/viewjob?jk=${jk}`, seed: {} }));
}

// ----- discovery: generic SERP with triple extraction -----
async function discoverSerp({ base, mobile, keyword, location, fromage, proxyConfig, wanted, maxPages = 40 }) {
    const foundJKs = new Set();
    let url = new URL('/jobs', base);
    if (keyword)  url.searchParams.set('q', keyword);
    if (location) url.searchParams.set('l', location);
    if (fromage)  url.searchParams.set('fromage', fromage);
    // broader results; avoid filters hiding posts
    url.searchParams.set('filter', '0');
    url.searchParams.set('radius', '25');

    for (let page = 0; page < maxPages && foundJKs.size < wanted; page++) {
        let html;
        try {
            html = await fetchHtml(url.href, { proxyConfig, mobile, attempts: 4, timeoutMs: 45000 });
        } catch (e) {
            log.warning(`${mobile ? 'Mobile' : 'Desktop'} SERP page ${page} fetch failed: ${e.message}`);
            break;
        }
        const $ = cheerio.load(html);

        const byDom = discoverJKsFromDom($, base);
        const byMosaic = discoverJKsFromMosaic(html);
        const byRegex = discoverJKsByRegex(html);

        const before = foundJKs.size;
        for (const jk of [...byDom, ...byMosaic, ...byRegex]) foundJKs.add(jk);
        const gained = foundJKs.size - before;

        log.info(`${mobile ? 'Mobile' : 'Desktop'} SERP page ${page}: DOM=${byDom.length}, Mosaic=${byMosaic.length}, Regex=${byRegex.length}, gained=${gained}, total=${foundJKs.size}`);

        // pagination
        let nextUrl;
        // Try standard pagination buttons
        const nextHref = $('a[aria-label="Next"], a[rel="next"], a[data-testid="pagination-page-next"]').attr('href');
        if (nextHref) nextUrl = new URL(nextHref, base).href;
        else {
            // fallback to start param
            const u = new URL(url.href);
            const start = parseInt(u.searchParams.get('start') || '0', 10);
            u.searchParams.set('start', String(start + 10));
            nextUrl = u.href;
        }
        if (!nextUrl || nextUrl === url.href) break;
        url = new URL(nextUrl);
    }
    return uniqueDetailTargets(Array.from(foundJKs).slice(0, wanted), mobile ? base.replace('www.', 'm.') : 'https://m.' + base.split('://')[1], base);
}

// ----- discovery: RSS (last resort) -----
async function discoverRss({ desktopBase, keyword, location, fromage, proxyConfig, wanted }) {
    const found = [];
    const rss = new URL('/rss', desktopBase);
    if (keyword)  rss.searchParams.set('q', keyword);
    if (location) rss.searchParams.set('l', location);
    if (fromage)  rss.searchParams.set('fromage', fromage);

    for (let i = 0; i < 5 && found.length === 0; i++) {
        try {
            await pace();
            const proxyUrl = proxyConfig ? await proxyConfig.newUrl() : undefined;
            const res = await gotScraping({
                url: rss.href,
                headers: { 'accept': 'application/rss+xml, application/xml;q=0.9,*/*;q=0.8' },
                headerGeneratorOptions: {
                    browsers: [{ name: 'chrome', minVersion: 118 }],
                    devices: ['desktop'],
                    operatingSystems: ['windows', 'macos'],
                },
                proxyUrl,
                http2: false,
                timeout: { request: 45000 },
                throwHttpErrors: false,
            });
            if (res.statusCode >= 500 || res.statusCode === 429) continue;
            if (res.statusCode >= 400) break;

            const $x = cheerio.load(res.body || '', { xmlMode: true });
            $x('item').each((_, el) => {
                if (found.length >= wanted) return;
                const $el = $x(el);
                const link = $el.find('link').text();
                const titleRaw = norm($el.find('title').text());
                const pubDate = norm($el.find('pubDate').text());
                const jk = jkFrom(link, desktopBase);
                if (!jk) return;

                const parts = titleRaw.split(' - ').map(norm).filter(Boolean);
                const seed = {
                    title: parts[0] || titleRaw || null,
                    company: parts.length >= 2 ? parts[parts.length - 2] : null,
                    location: parts.length >= 3 ? parts[parts.length - 1] : null,
                    date_posted: pubDate || null,
                };
                found.push({ url: `${desktopBase.replace('www.', 'm.')}/viewjob?jk=${jk}`, seed });
            });
        } catch { /* retry loop */ }
    }
    return found;
}

// ----- process one job detail (mobile-first, desktop fallback if thin) -----
async function fetchJob(detail, proxyConfig, mobileBase, desktopBase) {
    const { url, seed } = detail;
    const getJK = () => jkFrom(url, mobileBase) || url.match(/[?&]jk=([a-z0-9]+)/i)?.[1];

    let htmlMobile;
    try {
        htmlMobile = await fetchHtml(url, { proxyConfig, mobile: true, attempts: 4 });
    } catch (e) {
        try {
            const jk = getJK();
            if (!jk) throw e;
            const htmlDesk = await fetchHtml(`${desktopBase}/viewjob?jk=${jk}`, { proxyConfig, mobile: false, attempts: 4 });
            const $d = cheerio.load(htmlDesk);
            const res = extractDetail($d, url, seed);
            await Actor.pushData(res);
            return true;
        } catch (e2) {
            log.warning(`Detail failed: ${url} — ${e2.message}`);
            return false;
        }
    }

    let $ = cheerio.load(htmlMobile);
    const thin = !$('#jobDescriptionText, #jobDescriptionTextContainer').length && $.root().text().trim().length < 80;
    if (thin) {
        try {
            const jk = getJK();
            if (jk) {
                const htmlDesk = await fetchHtml(`${desktopBase}/viewjob?jk=${jk}`, { proxyConfig, mobile: false, attempts: 3 });
                $ = cheerio.load(htmlDesk);
            }
        } catch { /* keep mobile */ }
    }

    const res = extractDetail($, url, seed);
    await Actor.pushData(res);
    return true;
}

// ----------- MAIN -----------
await Actor.init();
const input = (await Actor.getInput()) || {};
const {
    keyword = 'office',
    location = 'United States',
    postedWithin = '7d',          // '24h'|'7d'|'30d'
    results_wanted = 50,
    maxConcurrency = 2,
    proxyConfiguration = null,
    indeedDomain = 'www.indeed.com'  // allow overrides like 'ca.indeed.com', 'uk.indeed.com'
} = input;

const proxyConfig = await Actor.createProxyConfiguration(proxyConfiguration);
const fromage = DATE_MAP[postedWithin] || '7';

const DESKTOP = `https://${indeedDomain}`;
const MOBILE  = indeedDomain.startsWith('m.') ? `https://${indeedDomain}` : `https://m.${indeedDomain.replace(/^www\./, '')}`;

log.info(`Indeed discovery: "${keyword}" in "${location}" (${postedWithin}) on ${indeedDomain}`);

// 1) Mobile SERP with triple extract
let targets = await discoverSerp({ base: MOBILE, mobile: true, keyword, location, fromage, proxyConfig, wanted: results_wanted, maxPages: 40 });

if (!targets.length) {
    log.warning('Mobile SERP yielded 0 — trying Desktop SERP with triple extract…');
    targets = await discoverSerp({ base: DESKTOP, mobile: false, keyword, location, fromage, proxyConfig, wanted: results_wanted, maxPages: 40 });
}

if (!targets.length) {
    log.warning('Desktop SERP yielded 0 — trying RSS…');
    targets = await discoverRss({ desktopBase: DESKTOP, keyword, location, fromage, proxyConfig, wanted: results_wanted });
}

if (!targets.length) {
    log.error('No jobs discovered after all strategies. Check proxies, domain, and inputs.');
    await Actor.exit();
}

// trim to requested count
targets = targets.slice(0, results_wanted);

// simple worker pool
async function runPool(items, worker, concurrency) {
    const queue = items.slice();
    const workers = Array.from({ length: concurrency }, async () => {
        while (queue.length) {
            const item = queue.shift();
            try { await worker(item); }
            catch (e) { /* already logged */ }
        }
    });
    await Promise.all(workers);
}

await runPool(
    targets,
    (it) => fetchJob(it, proxyConfig, MOBILE, DESKTOP),
    Math.max(1, Number(maxConcurrency) || 2)
);

log.info(`Done. Attempted ${targets.length} details.`);
await Actor.exit();
