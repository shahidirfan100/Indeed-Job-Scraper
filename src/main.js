// Indeed jobs scraper — HTTP requests + Cheerio only (no Crawlee request helpers)
// - RSS bootstrap for discovery (light + gives pubDate -> date_posted)
// - Mobile /viewjob?jk=... for details, desktop fallback inline if mobile HTML is thin
// - Apify proxy rotation + pacing + backoff to reduce 403/590
// - Never hit /rc/clk or /pagead/clk
// - Outputs: title, company, location, description_html, description_text, date_posted, job_types, url

import { Actor, log } from 'apify';
import { gotScraping } from 'got-scraping';
import * as cheerio from 'cheerio';

// ---------- Constants ----------
const DESKTOP = 'https://www.indeed.com';
const MOBILE  = 'https://m.indeed.com';
const DATE_MAP = { '24h': '1', '7d': '7', '30d': '30' };

// Pacing (helps with proxy churn / rate-limits)
const MIN_GAP_MS = 1100;
let lastTs = 0;
async function pace() {
    const now = Date.now();
    const diff = now - lastTs;
    if (diff < MIN_GAP_MS) await Actor.sleep(MIN_GAP_MS - diff);
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
const uaPick = (mobile=false) => (mobile ? UAS_MOBILE : UAS_DESKTOP)[Math.floor(Math.random() * (mobile ? UAS_MOBILE.length : UAS_DESKTOP.length))];

const baseHeaders = (ua) => ({
    'user-agent': ua,
    'accept-language': 'en-US,en;q=0.9',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'upgrade-insecure-requests': '1',
    'referer': 'https://www.google.com/',
    'cache-control': 'no-cache',
    'pragma': 'no-cache',
    'connection': 'close',     // keep HTTP/1.1 with some proxies
});

const jkFrom = (href) => {
    try { const u = new URL(href, DESKTOP); return u.searchParams.get('jk'); } catch { return null; }
};

// Simple promise pool (no external deps)
async function runPool(items, worker, concurrency) {
    const ret = [];
    let i = 0;
    const inFlight = new Set();
    async function spawn() {
        if (i >= items.length) return;
        const idx = i++;
        const p = (async () => {
            ret[idx] = await worker(items[idx], idx);
        })().finally(() => inFlight.delete(p));
        inFlight.add(p);
        if (inFlight.size >= concurrency) {
            await Promise.race(inFlight);
        }
        await spawn();
    }
    await spawn();
    await Promise.all(inFlight);
    return ret;
}

// HTTP fetch with retries + proxy rotation
async function fetchHtml(url, { proxyConfig, mobile=false, attempts=4 }) {
    let lastErr;
    for (let k = 0; k < attempts; k++) {
        try {
            await pace();
            const proxyUrl = proxyConfig ? await proxyConfig.newUrl() : undefined;
            const res = await gotScraping({
                url,
                headers: baseHeaders(uaPick(mobile)),
                proxyUrl,
                timeout: { request: 60000 },
                http2: false,
                throwHttpErrors: false,
            });

            const sc = res.statusCode || 0;
            if (sc === 403 || sc === 429) {
                lastErr = new Error(`HTTP ${sc}`);
                continue; // rotate and retry
            }
            if (sc >= 500) {
                lastErr = new Error(`HTTP ${sc}`);
                continue;
            }
            if (!res.body || res.body.length < 50) {
                lastErr = new Error(`Thin body (${res.body?.length || 0})`);
                continue;
            }
            return res.body;
        } catch (e) {
            lastErr = e;
        }
        // backoff with jitter
        const back = Math.min(6000, 700 * (1 << k)) + Math.floor(Math.random() * 200);
        await Actor.sleep(back);
    }
    throw lastErr || new Error('Failed to fetch');
}

// Parse “posted” safely from detail (no CSS noise)
function postedFromDetail($) {
    const t = norm($('div.jobsearch-JobMetadataFooter, [data-testid="jobsearch-JobMetadataFooter"]').text());
    if (!t) return null;
    const patterns = [
        /(Just posted|Today)/i,
        /Posted\s+\d+\+?\s+(?:day|days|hour|hours)\s+ago/i,
        /\d+\+?\s+(?:day|days|hour|hours)\s+ago/i,
        /Active\s+\d+\+?\s+(?:day|days|hour|hours)\s+ago/i,
    ];
    for (const re of patterns) { const m = t.match(re); if (m) return m[0]; }
    const m2 = t.match(/Posted[^|]+/i);
    return m2 ? m2[0].trim() : null;
}

const pickText = ($, sels) => {
    for (const s of sels) {
        const el = $(s).first();
        if (el.length) {
            const txt = norm(el.text());
            if (txt) return txt;
        }
    }
    return null;
};
const pickHtml = ($, sels) => {
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

    const title = pickText($, [
        'h1[data-testid="jobsearch-JobTitle"]',
        'h1.jobsearch-JobInfoHeader-title',
        'h1',
        'h1.jobsearch-JobInfoHeader-title-container',
    ]) ?? seed?.title ?? null;

    const company = pickText($, [
        '[data-company-name] a',
        '[data-company-name]',
        'div[data-testid="inlineHeader-companyName"]',
        '.jobsearch-CompanyInfoWithoutHeaderImage div a',
        '.jobsearch-CompanyInfoWithoutHeaderImage div',
        'div.jobsearch-CompanyInfoContainer a',
        'div.jobsearch-CompanyInfoContainer',
    ]) ?? seed?.company ?? null;

    const location = pickText($, [
        'div[data-testid="inlineHeader-companyLocation"]',
        '.jobsearch-CompanyInfoWithoutHeaderImage > div:last-child',
        'div.jobsearch-CompanyInfoContainer ~ div',
    ]) ?? seed?.location ?? null;

    const description_html = pickHtml($, [
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

// ----------- MAIN -----------
await Actor.init();
const input = (await Actor.getInput()) || {};

const {
    keyword = 'office',
    location = 'United States',
    postedWithin = '7d',          // '24h' | '7d' | '30d'
    results_wanted = 100,
    maxConcurrency = 2,           // keep conservative first
    proxyConfiguration = null,
} = input;

const proxyConfig = await Actor.createProxyConfiguration(proxyConfiguration);
const fromage = DATE_MAP[postedWithin] || '7';

// 1) RSS bootstrap (cheap & less blocked). Returns array of { url, seed }
async function discoverViaRss(limit) {
    const out = [];
    const rss = new URL('/rss', DESKTOP);
    if (keyword)  rss.searchParams.set('q', keyword);
    if (location) rss.searchParams.set('l', location);
    rss.searchParams.set('fromage', fromage);

    for (let i = 0; i < 5 && out.length === 0; i++) {
        try {
            const proxyUrl = proxyConfig ? await proxyConfig.newUrl() : undefined;
            await pace();
            const res = await gotScraping({
                url: rss.href,
                headers: {
                    'accept': 'application/rss+xml, application/xml;q=0.9, */*;q=0.8',
                    ...baseHeaders(uaPick(false)),
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
                if (out.length >= limit) return;
                const $el = $x(el);
                const link = $el.find('link').text();
                const titleRaw = norm($el.find('title').text());
                const pubDate = norm($el.find('pubDate').text());
                const jk = jkFrom(link);
                if (!jk) return;

                // Often formatted: "Role - Company - Location"
                const parts = titleRaw.split(' - ').map(norm).filter(Boolean);
                const seed = {
                    title: parts[0] || titleRaw || null,
                    company: parts.length >= 2 ? parts[parts.length - 2] : null,
                    location: parts.length >= 3 ? parts[parts.length - 1] : null,
                    date_posted: pubDate || null,
                };
                out.push({ url: `${MOBILE}/viewjob?jk=${jk}`, seed });
            });
        } catch {
            // try again
        }
    }
    return out;
}

// 2) Mobile SERP fallback if RSS empty
async function discoverViaMobileSerp(limit) {
    const out = [];
    let next = new URL('/jobs', MOBILE);
    if (keyword)  next.searchParams.set('q', keyword);
    if (location) next.searchParams.set('l', location);
    next.searchParams.set('fromage', fromage);

    while (out.length < limit) {
        let html;
        try {
            html = await fetchHtml(next.href, { proxyConfig, mobile: true, attempts: 4 });
        } catch (e) {
            log.warning(`SERP fetch failed (${e.message}).`);
            break;
        }
        const $ = cheerio.load(html);

        $('a[href*="/viewjob?jk="]').each((_, a) => {
            if (out.length >= limit) return;
            const href = $(a).attr('href');
            const jk = jkFrom(href);
            if (!jk) return;

            const $card = $(a).closest('li, div');
            const seed = {};
            const sTitle = norm($card.find('h2, h3, .jobTitle').first().text());
            const sCompany = norm($card.find('[data-company-name], .companyName').first().text());
            const sLoc = norm($card.find('[data-testid="text-location"], .companyLocation').first().text());
            const sPosted = norm(
                $card.find('span:contains("Posted"), span:contains("Just posted"), span:contains("Today"), span:contains("Active"), span.date, .css-1v0q9r6')
                    .first().text()
            );
            if (sTitle) seed.title = sTitle;
            if (sCompany) seed.company = sCompany;
            if (sLoc) seed.location = sLoc;
            if (sPosted) seed.date_posted = sPosted;

            out.push({ url: `${MOBILE}/viewjob?jk=${jk}`, seed });
        });

        // pagination
        let nextUrl;
        const nextHref = $('a[aria-label="Next"], a[data-testid="pagination-page-next"]').attr('href');
        if (nextHref) nextUrl = new URL(nextHref, MOBILE).href;
        else {
            const u = new URL(next.href);
            const start = parseInt(u.searchParams.get('start') || '0', 10);
            u.searchParams.set('start', String(start + 10));
            nextUrl = u.href;
        }
        if (!nextUrl || nextUrl === next.href) break;
        next = new URL(nextUrl);
    }

    return out;
}

// 3) Fetch & parse a single job (mobile first, desktop fallback if needed)
async function fetchJob(detail, idx) {
    const { url, seed } = detail;
    let html;
    try {
        html = await fetchHtml(url, { proxyConfig, mobile: true, attempts: 4 });
    } catch (e) {
        // try desktop directly if mobile keeps failing
        try {
            const jk = jkFrom(url) || url.match(/[?&]jk=([a-z0-9]+)/i)?.[1];
            if (!jk) throw e;
            html = await fetchHtml(`${DESKTOP}/viewjob?jk=${jk}`, { proxyConfig, mobile: false, attempts: 4 });
        } catch (e2) {
            log.warning(`Detail failed (${e2.message}) — ${url}`);
            return null;
        }
    }

    let $ = cheerio.load(html);
    const thin = !$('#jobDescriptionText, #jobDescriptionTextContainer').length && $.root().text().trim().length < 80;
    if (thin) {
        try {
            const jk = jkFrom(url) || url.match(/[?&]jk=([a-z0-9]+)/i)?.[1];
            if (jk) {
                const html2 = await fetchHtml(`${DESKTOP}/viewjob?jk=${jk}`, { proxyConfig, mobile: false, attempts: 3 });
                $ = cheerio.load(html2);
            }
        } catch { /* keep mobile content if any */ }
    }

    const result = extractDetail($, url, seed);
    await Actor.pushData(result);
    return result;
}

// ---------- Run ----------
try {
    log.info('Starting Indeed scraper (HTTP + Cheerio)…');

    // Discover targets
    let targets = await discoverViaRss(results_wanted);
    if (!targets.length) {
        log.info('RSS empty/blocked — falling back to mobile SERP discovery.');
        targets = await discoverViaMobileSerp(results_wanted);
    }
    if (!targets.length) {
        log.warning('No jobs discovered. Check keyword/location/postedWithin or proxy settings.');
        await Actor.exit();
    }

    // Trim to requested count
    targets = targets.slice(0, results_wanted);

    // Process with a small concurrency
    const concurrency = Math.max(1, Number(maxConcurrency) || 2);
    await runPool(targets, fetchJob, concurrency);

    log.info('Done.');
} finally {
    await Actor.exit();
}
