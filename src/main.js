import { Actor } from 'apify';
import { CheerioCrawler, Dataset, log } from 'crawlee';
import * as cheerio from 'cheerio';
import { gotScraping } from 'got-scraping';

// ——— CONSTANTS ———
const BASE_URL = 'https://www.indeed.com';
const DATE_POSTED_MAP = { '24h': '1', '7d': '7', '30d': '30' };

// ——— HELPERS ———
const norm = (t) => (t || '').replace(/\s+/g, ' ').trim();

const pickText = ($, selectors) => {
    for (const sel of selectors) {
        const txt = $(sel).first().text();
        if (txt && norm(txt)) return norm(txt);
    }
    return null;
};

const pickHtml = ($, selectors) => {
    for (const sel of selectors) {
        const html = $(sel).first().html();
        if (html && norm(html)) return html.trim();
    }
    return null;
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

// ——— MAIN ———
await Actor.init();

const input = (await Actor.getInput()) || {};
const {
    keyword,
    location = '',
    posted_date = 'anytime',    // 'anytime' | '24h' | '7d' | '30d'
    max_pages = 5,
    max_items = 200,
    proxyConfiguration,         // unchanged; driven by your input schema
} = input;

if (!keyword || !keyword.trim()) {
    throw new Error('Input "keyword" is required.');
}

// Build initial search URL
const startUrl = new URL('/jobs', BASE_URL);
startUrl.searchParams.set('q', keyword);
if (location) startUrl.searchParams.set('l', location);
if (posted_date !== 'anytime' && DATE_POSTED_MAP[posted_date]) {
    startUrl.searchParams.set('fromage', DATE_POSTED_MAP[posted_date]);
}

let itemCount = 0;
let pageCount = 0;

const crawler = new CheerioCrawler({
    useSessionPool: true,
    persistCookiesPerSession: true,
    proxyConfiguration: await Actor.createProxyConfiguration?.(proxyConfiguration),

    maxConcurrency: 2,
    maxRequestRetries: 3,
    requestHandlerTimeoutSecs: 60,

    // Supported way in CheerioCrawler 3.x: modify got (HTTP client) options here
    // (see docs/examples). We'll set headers and add a small jitter. :contentReference[oaicite:2]{index=2}
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
                // a benign referrer helps on list pages
                'Referer': 'https://www.google.com/',
            };

            // Gentle jitter (800–2200ms) between requests to reduce rate-based blocks
            const sleepMs = 800 + Math.floor(Math.random() * 1400);
            await Actor.sleep(sleepMs);
        },
    ],

    async requestHandler(ctx) {
        const { request, $, response, session, log, crawler } = ctx;
        const { label } = request.userData || {};

        // Early block detection (compatible with Crawlee 3.x)
        const status = response?.statusCode ?? response?.status;
        if (status === 403 || status === 429) {
            log.warning(`Blocked with status ${status} on ${request.url}. Marking session bad.`);
            session?.markBad();
            throw new Error(`Blocked ${status}`);
        }
        if ($) {
            const pageText = norm($.root().text().slice(0, 2000)).toLowerCase();
            if (/verify|robot|unusual traffic|are you a human|captcha/.test(pageText)) {
                log.warning(`Verification/bot page detected: ${request.url}`);
                session?.markBad();
                throw new Error('Bot/verification challenge');
            }
        }

        if (label === 'LIST') {
            pageCount += 1;
            log.info(`LIST page ${pageCount}: ${request.url}`);

            // Collect job detail links
            const jobLinks = [];
            $('a.tapItem[href], a[data-jk][href]').each((_, el) => {
                const href = $(el).attr('href');
                if (!href) return;
                if (/^https?:\/\/(www\.)?google\./i.test(href)) return; // skip ad redirects
                const abs = new URL(href, BASE_URL).href;
                jobLinks.push(abs);
            });

            for (const url of jobLinks) {
                if (itemCount >= max_items) break;
                await crawler.addRequests([{ url, userData: { label: 'DETAIL' } }]);
            }

            // Pagination (prefer "Next", else bump ?start= by 10)
            if (pageCount < max_pages && itemCount < max_items) {
                const nextSel = 'a[aria-label="Next"], a[data-testid="pagination-page-next"]';
                const nextHref = $(nextSel).attr('href');
                if (nextHref) {
                    const nextUrl = new URL(nextHref, BASE_URL).href;
                    await crawler.addRequests([{ url: nextUrl, userData: { label: 'LIST' } }]);
                } else {
                    const u = new URL(request.url);
                    const current = parseInt(u.searchParams.get('start') || '0', 10);
                    u.searchParams.set('start', String(current + 10));
                    await crawler.addRequests([{ url: u.href, userData: { label: 'LIST' } }]);
                }
            }

        } else {
            // DETAIL PAGE — if $ missing, fetch via got-scraping (same headers via UA)
            let $$ = $;
            if (!$$) {
                const ua = session?.userData?.ua || randomDesktopUA();
                const resp = await gotScraping({
                    url: request.url,
                    headers: {
                        'User-Agent': ua,
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                        'Accept-Language': 'en-US,en;q=0.9',
                        'Upgrade-Insecure-Requests': '1',
                        'Referer': 'https://www.google.com/',
                    },
                });
                if (resp.statusCode === 403 || resp.statusCode === 429) {
                    session?.markBad();
                    throw new Error(`Blocked ${resp.statusCode} on detail fetch`);
                }
                $$ = cheerio.load(resp.body);
            }

            // ——— Field extraction ———
            const title = pickText($$, [
                'h1[data-testid="jobsearch-JobTitle"]',
                'h1.jobsearch-JobInfoHeader-title',
                'h1',
            ]);

            const company = pickText($$, [
                '[data-company-name] a',
                '[data-company-name]',
                'div[data-testid="inlineHeader-companyName"]',
                '.jobsearch-CompanyInfoWithoutHeaderImage div a',
                '.jobsearch-CompanyInfoWithoutHeaderImage div',
            ]);

            const locationText = pickText($$, [
                'div[data-testid="inlineHeader-companyLocation"]',
                '.jobsearch-CompanyInfoWithoutHeaderImage > div:last-child',
            ]);

            const descriptionHtml = pickHtml($$, [
                '#jobDescriptionText',
                'div#jobDescriptionText',
                'section#jobDescriptionText',
            ]);

            const descriptionText = descriptionHtml
                ? norm($$('#jobDescriptionText').text())
                : pickText($$, ['#jobDescriptionText', 'section#jobDescriptionText', 'article']);

            const jobPosted =
                pickText($$, [
                    'div.jobsearch-JobMetadataFooter > div:last-child',
                    'div[data-testid="jobsearch-JobMetadataFooter"] span:contains("Posted")',
                    'span:contains("Just posted")',
                    'span:contains("Today")',
                    'span:contains("Active")',
                ]) || null;

            const jobTypes = (() => {
                const types = new Set();
                // Primary (newer detail grid)
                $$('div[data-testid="job-details"] div:contains("Job type")')
                    .next()
                    .find('li')
                    .each((_, li) => types.add(norm($$(li).text())));
                // Fallback token scan
                $$('li').each((_, li) => {
                    const t = norm($$(li).text());
                    if (/full[-\s]?time|part[-\s]?time|contract|temporary|internship|commission|per[-\s]?diem|apprenticeship|remote/i.test(t)) {
                        types.add(t);
                    }
                });
                const arr = Array.from(types).filter(Boolean);
                return arr.length ? arr : null;
            })();

            const out = {
                title: title || null,
                company: company || null,
                location: locationText || null,
                description_text: descriptionText || null,
                description_html: descriptionHtml || null,
                job_posted: jobPosted,
                job_types: jobTypes,
                url: request.url,
            };

            await Dataset.pushData(out);
            itemCount += 1;
            log.debug(`Saved item #${itemCount}: ${out.title || out.url}`);
        }
    },

    failedRequestHandler({ request, error, session, log }) {
        log.error(`Request failed after retries: ${request.url} — ${error && error.message}`);
        session?.markBad?.();
    },
});

// Kickoff
log.info(`Starting crawl with URL: ${startUrl.href}`);
await crawler.run([{ url: startUrl.href, userData: { label: 'LIST' } }]);

log.info(`Done. Saved ${itemCount} items across ${pageCount} list page(s).`);
await Actor.exit();
