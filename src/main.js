import { Actor } from 'apify';
import { CheerioCrawler, Dataset, log } from 'crawlee';
import * as cheerio from 'cheerio';
import { gotScraping } from 'got-scraping';

// ——— CONFIG ———
const BASE_URL = 'https://www.indeed.com';
const DATE_POSTED_MAP = { '24h': '1', '7d': '7', '30d': '30' };

// Helpers
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
    const bases = [
        // A few realistic UAs; one picked per session
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    ];
    return bases[Math.floor(Math.random() * bases.length)];
};

await Actor.init();

const input = (await Actor.getInput()) || {};
const {
    keyword,
    location = '',
    posted_date = 'anytime',       // 'anytime' | '24h' | '7d' | '30d'
    max_pages = 5,
    max_items = 200,
    proxyConfiguration,            // passthrough; unchanged
} = input;

if (!keyword || !keyword.trim()) {
    throw new Error('Input "keyword" is required.');
}

let itemCount = 0;
let pageCount = 0;

// Build initial search URL
const startUrl = new URL('/jobs', BASE_URL);
startUrl.searchParams.set('q', keyword);
if (location) startUrl.searchParams.set('l', location);
if (posted_date !== 'anytime' && DATE_POSTED_MAP[posted_date]) {
    startUrl.searchParams.set('fromage', DATE_POSTED_MAP[posted_date]);
}

// Crawler
const crawler = new CheerioCrawler({
    // Anti-block tuning
    useSessionPool: true,
    persistCookiesPerSession: true,
    maxRequestRetries: 3,
    maxConcurrency: 3,
    requestHandlerTimeoutSecs: 60,
    proxyConfiguration: await Actor.createProxyConfiguration?.(proxyConfiguration),

    // Set realistic headers *before* each navigation
    preNavigationHooks: [
        async ({ request, session }, gotoOptions) => {
            // Only adjust headers when we fetch HTML pages
            const ua = session?.userData?.ua || randomDesktopUA();
            if (session && !session.userData.ua) session.userData.ua = ua;

            request.headers = {
                ...request.headers,
                'User-Agent': ua,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Upgrade-Insecure-Requests': '1',
                'Cache-Control': 'no-cache',
                'Pragma': 'no-cache',
                'Referer': 'https://www.google.com/',
            };
        },
    ],

    // If we still get blocked, mark session bad and retry with a new one
    postResponseHooks: [
        async ({ response, session, request, log }) => {
            const status = response?.statusCode || response?.status;
            if (status === 403 || status === 429) {
                log.warning(`Blocked with status ${status} on ${request.url}. Marking session bad.`);
                session?.markBad();
                throw new Error(`Blocked ${status}`);
            }
        },
    ],

    async requestHandler(ctx) {
        const { request, $, response, session, log, crawler } = ctx;
        const { label } = request.userData || {};

        // Detect generic bot/verify pages
        const status = response?.statusCode || response?.status;
        if (status === 403 || status === 429) {
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

            // Enqueue detail links from job cards
            const jobLinks = [];
            $('a.tapItem[href], a[data-jk][href]').each((_, el) => {
                const href = $(el).attr('href');
                if (!href) return;
                const abs = new URL(href, BASE_URL).href;
                jobLinks.push(abs);
            });

            for (const url of jobLinks) {
                if (itemCount >= max_items) break;
                await crawler.addRequests([{ url, userData: { label: 'DETAIL' } }]);
            }

            // Pagination
            if (pageCount < max_pages && itemCount < max_items) {
                const nextSel = 'a[aria-label="Next"], a[data-testid="pagination-page-next"]';
                const nextHref = $(nextSel).attr('href');
                if (nextHref) {
                    const nextUrl = new URL(nextHref, BASE_URL).href;
                    await crawler.addRequests([{ url: nextUrl, userData: { label: 'LIST' } }]);
                } else {
                    // Fallback: increment ?start= param by 10
                    const u = new URL(request.url);
                    const current = parseInt(u.searchParams.get('start') || '0', 10);
                    u.searchParams.set('start', String(current + 10));
                    await crawler.addRequests([{ url: u.href, userData: { label: 'LIST' } }]);
                }
            }

        } else {
            // DETAIL page. If Crawlee didn't give us $, fetch manually then load.
            let $$ = $;
            if (!$$) {
                const resp = await gotScraping({
                    url: request.url,
                    headers: {
                        'User-Agent': session?.userData?.ua || randomDesktopUA(),
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

            // ——— FIELDS ———
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
                // Newer layout
                $$('div[data-testid="job-details"] div:contains("Job type")')
                    .next()
                    .find('li')
                    .each((_, li) => types.add(norm($$(li).text())));

                // Generic tokens
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
