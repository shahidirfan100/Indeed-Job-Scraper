// Node 22 / Apify SDK v3 / Crawlee v3 — HTTP-only Indeed scraper with CheerioCrawler
// Discovery: Mobile SERP → Desktop SERP; per page use DOM + Mosaic JSON + Regex to find JKs
// Details: Mobile /viewjob?jk=… first; Desktop fallback if description is thin
// Fields: title, company, location, description_html, description_text, date_posted, job_types, url

import { Actor, log } from 'apify';
import {
    CheerioCrawler,
    createCheerioRouter,
    Dataset,
    RequestQueue,
} from 'crawlee';
import * as cheerio from 'cheerio';

// ----------------- helpers -----------------
const DATE_MAP = { '24h': '1', '7d': '7', '30d': '30' };

const norm = (t) => (t || '').replace(/\s+/g, ' ').trim();
const stripDom = ($root) => { $root('script,style,noscript').remove(); };

function pickUA(isMobile) {
    const DESKTOP = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
    ];
    const MOBILE = [
        'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1',
        'Mozilla/5.0 (Linux; Android 14; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36',
        'Mozilla/5.0 (Linux; Android 14; SM-G990B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36',
    ];
    const arr = isMobile ? MOBILE : DESKTOP;
    return arr[Math.floor(Math.random() * arr.length)];
}

function jkFrom(href, base) {
    try { const u = new URL(href, base); return u.searchParams.get('jk'); } catch { return null; }
}

function discoverJKsFromDom($, base) {
    const set = new Set();
    $('a[href*="/viewjob?jk="]').each((_, a) => {
        const href = $(a).attr('href');
        const jk = jkFrom(href, base);
        if (jk) set.add(jk);
    });
    return Array.from(set);
}

function discoverJKsFromMosaic(html) {
    // Parse inline window.mosaic.providerData JSON without strict JSON.parse
    const set = new Set();
    const block =
        html.match(/window\.mosaic\.providerData\s*=\s*({[\s\S]*?});\s*<\/script>/i)
        || html.match(/"mosaic-provider-jobcards"[\s\S]*?({[\s\S]*?})\s*<\/script>/i);
    if (block) {
        const text = block[0];
        let m;
        const re1 = /"jk"\s*:\s*"([a-z0-9]{16})"/gi;
        while ((m = re1.exec(text)) !== null) set.add(m[1]);
        const re2 = /jobKeysWithInfo[^[]*\[([^\]]+)\]/gi;
        while ((m = re2.exec(text)) !== null) {
            const ids = (m[1].match(/"([a-z0-9]{16})"/gi) || []).map((s) => s.replace(/"/g, ''));
            for (const id of ids) set.add(id);
        }
    }
    return Array.from(set);
}

function discoverJKsByRegex(html) {
    const set = new Set();
    let m;
    const re = /\/viewjob\?[^"'<>]*?\bjk=([a-z0-9]{16})/gi;
    while ((m = re.exec(html)) !== null) set.add(m[1]);
    return Array.from(set);
}

const firstText = ($, sels) => {
    for (const s of sels) {
        const el = $(s).first();
        if (el.length) {
            const txt = norm(el.text());
            if (txt) return txt;
        }
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

    return {
        title, company, location,
        description_html: description_html || null,
        description_text,
        date_posted,
        job_types,
        url,
    };
}

// ----------------- main -----------------
await Actor.init();
const input = (await Actor.getInput()) || {};

const {
    keyword = 'office',
    location = 'United States',
    postedWithin = '7d',            // '24h'|'7d'|'30d'
    results_wanted = 100,
    maxPages = 40,
    maxConcurrency = 2,
    indeedDomain = 'www.indeed.com', // allow 'ca.indeed.com', 'uk.indeed.com', etc.
    proxyConfiguration = { useApifyProxy: true, apifyProxyGroups: ['RESIDENTIAL'], apifyProxyCountry: 'US' },
} = input;

const fromage = DATE_MAP[postedWithin] || '7';
const DESKTOP = `https://${indeedDomain}`;
const MOBILE  = indeedDomain.startsWith('m.') ? `https://${indeedDomain}` : `https://m.${indeedDomain.replace(/^www\./, '')}`;

const rq = await RequestQueue.open();
let discovered = 0;
let pushedDetails = 0;
let pageCount = 0;

// seed: start with MOBILE SERP
{
    const u = new URL('/jobs', MOBILE);
    if (keyword)  u.searchParams.set('q', keyword);
    if (location) u.searchParams.set('l', location);
    if (fromage)  u.searchParams.set('fromage', fromage);
    u.searchParams.set('filter', '0');
    u.searchParams.set('radius', '25');
    await rq.addRequest({ url: u.href, userData: { label: 'LIST', mobile: true, page: 0 } });
}

const router = createCheerioRouter();

// LIST handler: discover JKs and paginate
router.addHandler('LIST', async (ctx) => {
    const { request, $, body, response, session, log: clog, proxyInfo, crawler } = ctx;
    const { mobile, page } = request.userData || {};

    // Anti-block: handle HTTP status
    const status = response?.statusCode || response?.status;
    if (status === 403 || status === 429) {
        clog.warning(`Blocked ${status} on ${request.url} — rotate session`);
        session?.markBad?.();
        throw new Error(`Blocked ${status}`);
    }
    if (status >= 500) {
        session?.markBad?.();
        throw new Error(`Upstream ${status}`);
    }
    if (!body || body.length < 100) throw new Error('Thin SERP body');

    // extract JKs
    const base = mobile ? MOBILE : DESKTOP;
    const domJKs = discoverJKsFromDom($, base);
    const mosaicJKs = discoverJKsFromMosaic(body);
    const regexJKs = discoverJKsByRegex(body);

    const allJKs = Array.from(new Set([...domJKs, ...mosaicJKs, ...regexJKs]));
    discovered += allJKs.length;
    clog.info(`${mobile ? 'Mobile' : 'Desktop'} SERP page ${page}: DOM=${domJKs.length} Mosaic=${mosaicJKs.length} Regex=${regexJKs.length} totalJKs=${allJKs.length} discoveredSoFar=${discovered}`);

    // push detail requests (mobile detail first)
    for (const jk of allJKs) {
        if (pushedDetails >= results_wanted) break;
        const detailUrl = `${MOBILE}/viewjob?jk=${jk}`;
        await rq.addRequest({
            url: detailUrl,
            userData: { label: 'DETAIL', jk, triedDesktop: false, seed: {} },
        });
        pushedDetails++;
    }

    // stop if enough
    if (pushedDetails >= results_wanted) return;

    // paginate
    if (page + 1 < maxPages) {
        // Prefer explicit "Next" if present
        const nextHref = $('a[aria-label="Next"], a[rel="next"], a[data-testid="pagination-page-next"]').attr('href');
        let nextUrl;
        if (nextHref) nextUrl = new URL(nextHref, base).href;
        else {
            const u = new URL(request.url);
            const start = parseInt(u.searchParams.get('start') || '0', 10);
            u.searchParams.set('start', String(start + 10));
            nextUrl = u.href;
        }
        if (nextUrl && nextUrl !== request.url) {
            await rq.addRequest({ url: nextUrl, userData: { label: 'LIST', mobile, page: page + 1 } });
            pageCount++;
        } else if (mobile) {
            // Switch to DESKTOP SERP if mobile has no next
            const du = new URL('/jobs', DESKTOP);
            if (keyword)  du.searchParams.set('q', keyword);
            if (location) du.searchParams.set('l', location);
            if (fromage)  du.searchParams.set('fromage', fromage);
            du.searchParams.set('filter', '0');
            du.searchParams.set('radius', '25');
            await rq.addRequest({ url: du.href, userData: { label: 'LIST', mobile: false, page: 0 } });
        }
    } else if (mobile) {
        // hit desktop after mobile maxPages
        const du = new URL('/jobs', DESKTOP);
        if (keyword)  du.searchParams.set('q', keyword);
        if (location) du.searchParams.set('l', location);
        if (fromage)  du.searchParams.set('fromage', fromage);
        du.searchParams.set('filter', '0');
        du.searchParams.set('radius', '25');
        await rq.addRequest({ url: du.href, userData: { label: 'LIST', mobile: false, page: 0 } });
    }
});

// DETAIL handler: extract; fallback to desktop if thin
router.addHandler('DETAIL', async (ctx) => {
    const { request, $, body, response, session, log: clog } = ctx;
    const { jk, triedDesktop, seed } = request.userData || {};

    const status = response?.statusCode || response?.status;
    if (status === 403 || status === 429) {
        clog.warning(`Detail blocked ${status} — ${request.url}`);
        session?.markBad?.();
        throw new Error(`Blocked ${status}`);
    }
    if (status >= 500) {
        session?.markBad?.();
        throw new Error(`Upstream ${status}`);
    }
    if (!body || body.length < 100) throw new Error('Thin detail body');

    const hasDesc = $('#jobDescriptionText, #jobDescriptionTextContainer').length > 0 || norm($.root().text()).length > 150;
    if (!hasDesc && !triedDesktop && jk) {
        // re-queue desktop detail
        const du = `${DESKTOP}/viewjob?jk=${jk}`;
        await ctx.crawler.requestQueue.addRequest({
            url: du,
            userData: { label: 'DETAIL', jk, triedDesktop: true, seed },
        });
        return;
    }

    const item = extractDetail($, request.url, seed);
    await Dataset.pushData(item);
});

// ----------------- crawler -----------------
const proxyConfig = await Actor.createProxyConfiguration(proxyConfiguration);

const crawler = new CheerioCrawler({
    requestQueue: rq,
    proxyConfiguration: proxyConfig,
    useSessionPool: true,
    // keep these within supported HttpCrawlerOptions
    maxConcurrency: Math.max(1, Number(maxConcurrency) || 2),
    requestHandler: router,
    navigationTimeoutSecs: 60,
    requestHandlerTimeoutSecs: 120,
    maxRequestRetries: 3,
    // gentle pacing + realistic headers
    preNavigationHooks: [
        async ({ request }) => {
            const isMobile = request.url.startsWith(MOBILE);
            request.headers = {
                ...request.headers,
                'user-agent': pickUA(isMobile),
                'accept-language': 'en-US,en;q=0.9',
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'upgrade-insecure-requests': '1',
                'referer': 'https://www.google.com/',
                'cache-control': 'no-cache',
                'pragma': 'no-cache',
                'connection': 'close',
            };
            // Small randomized delay to reduce burstiness
            const jitter = 500 + Math.floor(Math.random() * 900);
            await Actor.sleep(jitter);
        },
    ],
    failedRequestHandler: async ({ request, error, log: clog }) => {
        clog.warning(`Failed after retries: ${request.url} — ${error?.message || error}`);
    },
});

log.info(`Indeed: "${keyword}" in "${location}" within ${postedWithin} on ${indeedDomain}`);
await crawler.run();

// Safety net: if nothing was pushed, tell the logs but exit 0 (actor succeeds, dataset may be empty)
const info = await Dataset.getInfo();
if (!info?.itemCount) {
    log.error('No jobs discovered. Check proxy country (US), domain (www.indeed.com), and try smaller concurrency (1).');
}

await Actor.exit();
