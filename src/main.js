// src/main.js — Indeed scraper aligned with your Workable actor’s contract
import { Actor } from 'apify';
import { CheerioCrawler, Dataset, log, sleep } from 'crawlee';
import * as cheerio from 'cheerio';

// -------- Constants --------
const BASE_URL = 'https://www.indeed.com';
const DATE_MAP = { '24h': '1', '7d': '7', '30d': '30' };

// -------- Helpers --------
const norm = (t) => (t || '').replace(/\s+/g, ' ').trim();
const stripDomNoise = ($root) => {
    $root('script, style, noscript').remove();
};
const randomDesktopUA = () => {
    const uas = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
    ];
    return uas[Math.floor(Math.random() * uas.length)];
};

const pickText = ($, selectors) => {
    for (const sel of selectors) {
        const el = $(sel).first();
        if (el.length) {
            const txt = norm(el.text());
            if (txt) return txt;
        }
    }
    return null;
};

const pickHtml = ($, selectors) => {
    for (const sel of selectors) {
        const el = $(sel).first();
        if (el.length) {
            const html = el.html();
            if (html && norm(html)) return html.trim();
        }
    }
    return null;
};

// Tight date parser to avoid CSS dumps
const extractPosted = ($) => {
    // Only read metadata footer; do not read whole page text
    const footerTxt = norm($('div.jobsearch-JobMetadataFooter, [data-testid="jobsearch-JobMetadataFooter"]').text());
    if (!footerTxt) return null;

    // Common patterns
    const patterns = [
        /(Just posted|Today)/i,
        /Posted\s+\d+\+?\s+(?:day|days|hour|hours)\s+ago/i,
        /\d+\+?\s+(?:day|days|hour|hours)\s+ago/i,
        /Active\s+\d+\+?\s+(?:day|days|hour|hours)\s+ago/i,
    ];
    for (const re of patterns) {
        const m = footerTxt.match(re);
        if (m) return m[0];
    }
    // Fallback: anything containing 'Posted'
    const m2 = footerTxt.match(/Posted[^|]+/i);
    return m2 ? m2[0].trim() : null;
};

// Extracts fields on detail page
const extractDetail = ($, url, seed) => {
    stripDomNoise($);

    const title = pickText($, [
        'h1[data-testid="jobsearch-JobTitle"]',
        'h1.jobsearch-JobInfoHeader-title',
        'h1',
    ]) ?? seed?.title ?? null;

    const company = pickText($, [
        '[data-company-name] a',
        '[data-company-name]',
        'div[data-testid="inlineHeader-companyName"]',
        '.jobsearch-CompanyInfoWithoutHeaderImage div a',
        '.jobsearch-CompanyInfoWithoutHeaderImage div',
    ]) ?? seed?.company ?? null;

    const location = pickText($, [
        'div[data-testid="inlineHeader-companyLocation"]',
        '.jobsearch-CompanyInfoWithoutHeaderImage > div:last-child',
    ]) ?? seed?.location ?? null;

    const description_html = pickHtml($, [
        '#jobDescriptionText',
        'div#jobDescriptionText',
        'section#jobDescriptionText',
    ]);

    let description_text = null;
    if (description_html) {
        try {
            const $$ = cheerio.load(description_html);
            $$('script, style, noscript').remove();
            description_text = norm($$.text());
        } catch {
            description_text = norm($('#jobDescriptionText, section#jobDescriptionText').text()) || null;
        }
    } else {
        description_text = norm($('#jobDescriptionText, section#jobDescriptionText, article').text()) || null;
    }

    const date_posted = extractPosted($) ?? seed?.date_posted ?? null;

    const job_types = (() => {
        const types = new Set();
        $('div[data-testid="job-details"] div:contains("Job type")')
            .next()
            .find('li')
            .each((_, li) => types.add(norm($(li).text())));
        // Fallback: scan list tokens
        $('li').each((_, li) => {
            const t = norm($(li).text());
            if (/full[-\s]?time|part[-\s]?time|contract|temporary|intern(ship)?|commission|per[-\s]?diem|apprenticeship|remote/i.test(t)) {
                types.add(t);
            }
        });
        const arr = Array.from(types).filter(Boolean);
        return arr.length ? arr : null;
    })();

    return {
        title,
        company,
        location,
        date_posted,
        description_html: description_html || null,
        description_text,
        job_types,
        url,
    };
};

// -------- Main --------
await Actor.init();

// Inputs: keep EXACTLY your Workable contract
const input = (await Actor.getInput()) || {};
const {
    keyword = '',
    location = '',
    postedWithin = '7d',     // '24h' | '7d' | '30d'
    results_wanted = 200,
    maxConcurrency = 5,
    proxyConfiguration = null,
} = input;

const proxyConfig = await Actor.createProxyConfiguration(proxyConfiguration);

// Build start URL
const startUrl = new URL('/jobs', BASE_URL);
if (keyword) startUrl.searchParams.set('q', keyword);
if (location) startUrl.searchParams.set('l', location);
const fromage = DATE_MAP[postedWithin];
if (fromage) startUrl.searchParams.set('fromage', fromage);

// State
const state = { enqueued: 0, saved: 0, pages: 0 };

// Crawler
const crawler = new CheerioCrawler({
    proxyConfiguration: proxyConfig,
    maxConcurrency,
    useSessionPool: true,
    persistCookiesPerSession: true,
    maxRequestRetries: 3,
    requestHandlerTimeoutSecs: 60,

    // Set headers and pace safely (Crawlee 3.x supported)
    preNavigationHooks: [
        async (ctx, gotOptions) => {
            const { session } = ctx;
            const ua = session?.userData?.ua || randomDesktopUA();
            if (session && !session.userData.ua) session.userData.ua = ua;

            gotOptions.headers = {
                ...(gotOptions.headers || {}),
                'User-Agent': ua,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Cache-Control': 'no-cache',
                'Pragma': 'no-cache',
                'Upgrade-Insecure-Requests': '1',
                'Referer': 'https://www.google.com/',
            };

            // Random jitter
            await sleep(800 + Math.floor(Math.random() * 1400));
        },
    ],

    async requestHandler({ request, $, addRequests, response, session, log }) {
        const { label } = request.userData || {};

        // Early block detection
        const status = response?.statusCode ?? response?.status;
        if (status === 403 || status === 429) {
            session?.markBad();
            throw new Error(`Blocked ${status}`);
        }

        if (label === 'LIST') {
            state.pages += 1;
            log.info(`LIST page ${state.pages}: ${request.url}`);

            // Build canonical detail URLs from data-jk to avoid /rc/clk and /pagead/clk 403s
            const detailReqs = [];
            $('a.tapItem, a[data-jk]').each((_, el) => {
                const $a = $(el);
                const jk = $a.attr('data-jk') || $a.closest('[data-jk]').attr('data-jk');
                if (!jk) return;

                const detailUrl = `${BASE_URL}/viewjob?jk=${jk}`;
                // Stop if limit reached
                if (state.enqueued >= results_wanted) return;

                // Optional seeds from list card (helps if detail blocks)
                const seed = {};
                const cardTitle = norm($a.find('h2, h3, .jobTitle').first().text());
                if (cardTitle) seed.title = cardTitle;

                const company = norm($a.find('[data-company-name], .companyName').first().text());
                if (company) seed.company = company;

                const loc = norm($a.find('[data-testid="text-location"], .companyLocation').first().text());
                if (loc) seed.location = loc;

                // Sometimes list card has “Posted …”
                const posted = norm($a.find('span:contains("Posted"), span:contains("Just posted"), span:contains("Today")').first().text());
                if (posted) seed.date_posted = posted;

                detailReqs.push({ url: detailUrl, userData: { label: 'DETAIL', ...seed } });
                state.enqueued += 1;
            });

            if (detailReqs.length) await addRequests(detailReqs);
            if (state.enqueued >= results_wanted) return;

            // Pagination: prefer Next; else bump ?start= by 10
            const nextHref = $('a[aria-label="Next"], a[data-testid="pagination-page-next"]').attr('href');
            let nextUrl;
            if (nextHref) {
                nextUrl = new URL(nextHref, BASE_URL).href;
            } else {
                const u = new URL(request.url);
                const current = parseInt(u.searchParams.get('start') || '0', 10);
                u.searchParams.set('start', String(current + 10));
                nextUrl = u.href;
            }
            await addRequests([{ url: nextUrl, userData: { label: 'LIST' } }]);

        } else {
            // DETAIL
            let $$ = $;
            if (!$$) {
                // Fallback: re-fetch with same session/headers
                const resp = await this.sendRequest({ url: request.url, responseType: 'text' });
                if (resp.statusCode === 403 || resp.statusCode === 429) {
                    session?.markBad();
                    throw new Error(`Blocked ${resp.statusCode} on detail`);
                }
                $$ = cheerio.load(resp.body);
            }

            const result = extractDetail($$, request.url, request.userData);
            await Dataset.pushData(result);
            state.saved += 1;
            log.debug(`Saved: ${result.title || '(untitled)'} — ${result.company || ''}`);
        }
    },

    failedRequestHandler({ request, error, session, log }) {
        log.error(`Request failed after retries: ${request.url} — ${error && error.message}`);
        session?.markBad?.();
    },
});

// Kickoff
await crawler.addRequests([{ url: startUrl.href, userData: { label: 'LIST' } }]);
await crawler.run();

log.info(`Done. Enqueued ${state.enqueued}, saved ${state.saved}, pages ${state.pages}.`);
await Actor.exit();
