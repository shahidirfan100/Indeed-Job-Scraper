// Indeed jobs scraper — HTTP requests + Cheerio (no Crawlee request helpers).
// Discovery order: Mobile SERP -> Desktop SERP -> RSS (as last resort).
// Detail fetch: Mobile /viewjob?jk=… first; Desktop /viewjob?jk=… fallback if thin.
// Avoids /rc/clk and /pagead/clk (403 hotspots).
// Anti-blocking: Apify proxy rotation per attempt, randomized UA, pacing, backoff.
// Exports: title, company, location, description_html, description_text, date_posted, job_types, url.

import { Actor, log } from 'apify';
import { gotScraping } from 'got-scraping';
import * as cheerio from 'cheerio';

// ------------ constants ------------
const DESKTOP = 'https://www.indeed.com';
const MOBILE  = 'https://m.indeed.com';
const DATE_MAP = { '24h': '1', '7d': '7', '30d': '30' };

// global pacing (keeps proxy/session churn low)
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

const UAS_DESKTOP = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
];
const UAS_MOBILE = [
    'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (Linux; Android 14; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36',
    'Mozilla/5.0 (Linux; Android 14; SM-G990B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36',
];
const pickUA = (mobile=false) => (mobile ? UAS_MOBILE : UAS_DESKTOP)[Math.floor(Math.random() * (mobile ? UAS_MOBILE.length : UAS_DESKTOP.length))];

const baseHeaders = (ua) => ({
    'user-agent': ua,
    'accept-language': 'en-US,en;q=0.9',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'upgrade-insecure-requests': '1',
    'referer': 'https://www.google.com/',
    'cache-control': 'no-cache',
    'pragma': 'no-cache',
    'connection': 'close',
});

const jkFrom = (href) => {
    try { const u = new URL(href, DESKTOP); return u.searchParams.get('jk'); } catch { return null; }
};

async function fetchHtml(url, { proxyConfig, mobile=false, attempts=4, timeoutMs=60000 }) {
    let lastErr;
    for (let i = 0; i < attempts; i++) {
        try {
            await pace();
            const proxyUrl = proxyConfig ? await proxyConfig.newUrl() : undefined;
            const res = await gotScraping({
                url,
                headers: baseHeaders(pickUA(mobile)),
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

// --- posted date only from footer text (never from style blocks) ---
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

// ----- discovery: MOBILE SERP -----
async function discoverMobile({ keyword, location, fromage, proxyConfig, wanted, maxPages = 30 }) {
    const found = [];
    let url = new URL('/jobs', MOBILE);
    if (keyword)  url.searchParams.set('q', keyword);
    if (location) url.searchParams.set('l', location);
    if (fromage)  url.searchParams.set('fromage', fromage);

    for (let page = 0; page < maxPages && found.length < wanted; page++) {
        let html;
        try {
            html = await fetchHtml(url.href, { proxyConfig, mobile: true, attempts: 4, timeoutMs: 45000 });
        } catch (e) {
            log.warning(`Mobile SERP page ${page} fetch failed: ${e.message}`);
            break;
        }
        const $ = cheerio.load(html);

        // robust card discovery
        $('a[href*="/viewjob?jk="]').each((_, a) => {
            if (found.length >= wanted) return;
            const href = $(a).attr('href');
            const jk = jkFrom(href);
            if (!jk) return;
            const detailUrl = `${MOBILE}/viewjob?jk=${jk}`;

            const $card = $(a).closest('li, div');
            const seed = {};
            const sTitle = firstText($card, ['h2', 'h3', '.jobTitle']);
            const sCompany = firstText($card, ['[data-company-name]', '.companyName']);
            const sLoc = firstText($card, ['[data-testid="text-location"]', '.companyLocation']);
            const sPosted = firstText($card, [
                'span:contains("Posted")',
                'span:contains("Just posted")',
                'span:contains("Today")',
                'span:contains("Active")',
                'span.date',
                '.css-1v0q9r6',
            ]);

            if (sTitle) seed.title = sTitle;
            if (sCompany) seed.company = sCompany;
            if (sLoc) seed.location = sLoc;
            if (sPosted) seed.date_posted = sPosted;

            found.push({ url: detailUrl, seed });
        });

        // pagination
        let nextUrl;
        const nextHref = $('a[aria-label="Next"], a[data-testid="pagination-page-next"]').attr('href');
        if (nextHref) nextUrl = new URL(nextHref, MOBILE).href;
        else {
            const u = new URL(url.href);
            const start = parseInt(u.searchParams.get('start') || '0', 10);
            u.searchParams.set('start', String(start + 10));
            nextUrl = u.href;
        }
        if (!nextUrl || nextUrl === url.href) break;
        url = new URL(nextUrl);
    }
    return found;
}

// ----- discovery: DESKTOP SERP (fallback) -----
async function discoverDesktop({ keyword, location, fromage, proxyConfig, wanted, maxPages = 25 }) {
    const found = [];
    let url = new URL('/jobs', DESKTOP);
    if (keyword)  url.searchParams.set('q', keyword);
    if (location) url.searchParams.set('l', location);
    if (fromage)  url.searchParams.set('fromage', fromage);

    for (let page = 0; page < maxPages && found.length < wanted; page++) {
        let html;
        try {
            html = await fetchHtml(url.href, { proxyConfig, mobile: false, attempts: 4, timeoutMs: 45000 });
        } catch (e) {
            log.warning(`Desktop SERP page ${page} fetch failed: ${e.message}`);
            break;
        }
        const $ = cheerio.load(html);

        $('a[href*="/viewjob?jk="]').each((_, a) => {
            if (found.length >= wanted) return;
            const href = $(a).attr('href');
            const jk = jkFrom(href);
            if (!jk) return;
            const detailUrl = `${MOBILE}/viewjob?jk=${jk}`; // still prefer mobile for detail

            const $card = $(a).closest('div, li');
            const seed = {};
            const sTitle = firstText($card, ['h2', 'h3', 'span.jobTitle']);
            const sCompany = firstText($card, ['[data-company-name]', '.companyName']);
            const sLoc = firstText($card, ['[data-testid="text-location"]', '.companyLocation', '.company_location']);
            const sPosted = firstText($card, [
                'span:contains("Posted")',
                'span:contains("Just posted")',
                'span:contains("Today")',
                'span:contains("Active")',
                'span.date',
            ]);
            if (sTitle) seed.title = sTitle;
            if (sCompany) seed.company = sCompany;
            if (sLoc) seed.location = sLoc;
            if (sPosted) seed.date_posted = sPosted;

            found.push({ url: detailUrl, seed });
        });

        // pagination
        let nextUrl;
        const nextHref = $('a[aria-label="Next"], a[data-testid="pagination-page-next"]').attr('href');
        if (nextHref) nextUrl = new URL(nextHref, DESKTOP).href;
        else {
            const u = new URL(url.href);
            const start = parseInt(u.searchParams.get('start') || '0', 10);
            u.searchParams.set('start', String(start + 10));
            nextUrl = u.href;
        }
        if (!nextUrl || nextUrl === url.href) break;
        url = new URL(nextUrl);
    }
    return found;
}

// ----- discovery: RSS (last resort) -----
async function discoverRss({ keyword, location, fromage, proxyConfig, wanted }) {
    const found = [];
    const rss = new URL('/rss', DESKTOP);
    if (keyword)  rss.searchParams.set('q', keyword);
    if (location) rss.searchParams.set('l', location);
    if (fromage)  rss.searchParams.set('fromage', fromage);

    for (let i = 0; i < 5 && found.length === 0; i++) {
        try {
            await pace();
            const proxyUrl = proxyConfig ? await proxyConfig.newUrl() : undefined;
            const res = await gotScraping({
                url: rss.href,
                headers: { 'accept': 'application/rss+xml, application/xml;q=0.9,*/*;q=0.8', ...baseHeaders(pickUA(false)) },
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
                const jk = jkFrom(link);
                if (!jk) return;

                const parts = titleRaw.split(' - ').map(norm).filter(Boolean);
                const seed = {
                    title: parts[0] || titleRaw || null,
                    company: parts.length >= 2 ? parts[parts.length - 2] : null,
                    location: parts.length >= 3 ? parts[parts.length - 1] : null,
                    date_posted: pubDate || null,
                };
                found.push({ url: `${MOBILE}/viewjob?jk=${jk}`, seed });
            });
        } catch {
            // loop again
        }
    }
    return found;
}

// ----- process one job detail (mobile-first, desktop fallback if thin) -----
async function fetchJob(detail, proxyConfig) {
    const { url, seed } = detail;

    const getJK = () => jkFrom(url) || url.match(/[?&]jk=([a-z0-9]+)/i)?.[1];

    let htmlMobile;
    try {
        htmlMobile = await fetchHtml(url, { proxyConfig, mobile: true, attempts: 4 });
    } catch (e) {
        // try desktop directly if mobile failed repeatedly
        try {
            const jk = getJK();
            if (!jk) throw e;
            const htmlDesk = await fetchHtml(`${DESKTOP}/viewjob?jk=${jk}`, { proxyConfig, mobile: false, attempts: 4 });
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
                const htmlDesk = await fetchHtml(`${DESKTOP}/viewjob?jk=${jk}`, { proxyConfig, mobile: false, attempts: 3 });
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
    postedWithin = '7d',       // '24h'|'7d'|'30d'
    results_wanted = 100,
    maxConcurrency = 2,        // keep conservative; raise after stable
    proxyConfiguration = null,
} = input;

const proxyConfig = await Actor.createProxyConfiguration(proxyConfiguration);
const fromage = DATE_MAP[postedWithin] || '7';

log.info(`Indeed: keyword="${keyword}" location="${location}" postedWithin="${postedWithin}" wanted=${results_wanted}`);

// 1) Mobile SERP discovery
let targets = await discoverMobile({ keyword, location, fromage, proxyConfig, wanted: results_wanted, maxPages: 30 });
if (!targets.length) {
    log.warning('Mobile SERP yielded 0 — trying Desktop SERP...');
    targets = await discoverDesktop({ keyword, location, fromage, proxyConfig, wanted: results_wanted, maxPages: 25 });
}
if (!targets.length) {
    log.warning('Desktop SERP yielded 0 — trying RSS...');
    targets = await discoverRss({ keyword, location, fromage, proxyConfig, wanted: results_wanted });
}

if (!targets.length) {
    log.error('No jobs discovered after all strategies. Check proxies and inputs.');
    await Actor.exit();
}

// trim to requested count
targets = targets.slice(0, results_wanted);

// simple pool runner (no external libs)
async function runPool(items, worker, concurrency) {
    const queue = items.slice();
    const workers = Array.from({ length: concurrency }, async () => {
        while (queue.length) {
            const item = queue.shift();
            try { await worker(item); }
            catch (e) { /* already logged inside */ }
        }
    });
    await Promise.all(workers);
}

await runPool(targets, (it) => fetchJob(it, proxyConfig), Math.max(1, Number(maxConcurrency) || 2));

log.info(`Done. Attempted ${targets.length} details.`);
await Actor.exit();
