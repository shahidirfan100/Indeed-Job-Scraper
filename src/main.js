import { Actor } from 'apify';
import { CheerioCrawler, Dataset, log } from 'crawlee';
import * as cheerio from 'cheerio';

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

await Actor.init();

const input = (await Actor.getInput()) || {};
const {
    keyword,
    location = '',
    posted_date = 'anytime',    // 'anytime' | '24h' | '7d' | '30d'
    max_pages = 5,
    max_items = 200,
    proxyConfiguration,         // keep existing behavior
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

// CheerioCrawler (no unsupported hooks)
const crawler = new CheerioCrawler({
    // Anti-block basics compatible with your Crawlee version
    useSessionPool: true,
    persistCookiesPerSession: true,
    maxRequestRetries: 3,
    maxConcurrency: 3,
    requestHandlerTimeoutSecs: 60,
    proxyConfiguration: await Actor.createProxyConfiguration?.(proxyConfiguration),

    // Set realistic headers via header generator (works in Crawlee 3.x)
    headerGeneratorOptions: {
        browsers: [
            { name: 'chrome', minVersion: 120, maxVersion: 124 },
            { name: 'firefox', minVersion: 120, maxVersion: 124 },
            { name: 'safari', minVersion: 17, maxVersion: 17 },
        ],
        devices: ['desktop'],
        operatingSystems: ['windows', 'macos', 'linux'],
    },

    // Main handler
    async requestHandler(ctx) {
        const { request, $, response, session, log, crawler } = ctx;
        const { label } = request.userData || {};

        // Early block detection compatible with this API
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

            // Pagination: prefer "Next" button, fallback to ?start=
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
            // DETAIL PAGE

            // In case $ was not provided for some mime-types, load manually
            let $$ = $;
            if (!$$) {
                const body = (await ctx.sendRequest({ // uses underlying http client with same headers/sessions
                    url: request.url,
                    responseType: 'text',
                })).body;
                $$ = cheerio.load(body);
            }

            // ——— Field extraction with fallbacks ———
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
                // Newer layout (Job details grid)
                $$('div[data-testid="job-details"] div:contains("Job type")')
                    .next()
                    .find('li')
                    .each((_, li) => types.add(norm($$(li).text())));

                // Generic tokens as fallback
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
