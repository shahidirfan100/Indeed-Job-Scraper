import { Actor } from 'apify';
import { CheerioCrawler, Dataset, ProxyConfiguration, log } from 'crawlee';
import { gotScraping } from 'got-scraping';

// NOTE: Keep other actor settings intact; only this file is updated for Indeed.
const BASE_URL = 'https://www.indeed.com';

const DATE_POSTED_MAP = {
    '24h': '1',
    '7d': '7',
    '30d': '30',
};

/** Utility to normalize whitespace */
const norm = (t) => (t || '').replace(/\s+/g, ' ').trim();

/** Extract text with multiple fallbacks */
const pickText = ($, selectors) => {
    for (const sel of selectors) {
        const txt = $(sel).first().text();
        if (txt && norm(txt)) return norm(txt);
    }
    return null;
};

/** Extract HTML with fallbacks */
const pickHtml = ($, selectors) => {
    for (const sel of selectors) {
        const html = $(sel).first().html();
        if (html && norm(html)) return html.trim();
    }
    return null;
};

await Actor.init();

const input = (await Actor.getInput()) || {};

// Preserve existing input contract as much as possible.
const {
    keyword,
    location = '',
    posted_date = 'anytime', // 'anytime' | '24h' | '7d' | '30d'
    max_pages = 5,
    max_items = 200,
    proxyConfiguration,
} = input;

if (!keyword || !keyword.trim()) {
    throw new Error('Input "keyword" is required.');
}

const proxyConf = await Actor.createProxyConfiguration?.(proxyConfiguration);

/** Build initial search URL */
const startUrl = new URL('/jobs', BASE_URL);
startUrl.searchParams.set('q', keyword);
if (location) startUrl.searchParams.set('l', location);
if (posted_date !== 'anytime' && DATE_POSTED_MAP[posted_date]) {
    startUrl.searchParams.set('fromage', DATE_POSTED_MAP[posted_date]);
}

log.info(`Starting crawl at: ${startUrl.href}`);

let itemCount = 0;
let pageCount = 0;

const crawler = new CheerioCrawler({
    requestHandlerTimeoutSecs: 60,
    maxConcurrency: 5,
    proxyConfiguration: proxyConf,
    async requestHandler({ request, $, enqueueLinks, log, crawler }) {
        const { label } = request.userData || {};
        if (label === 'LIST') {
            pageCount += 1;
            log.info(`LIST page ${pageCount}: ${request.url}`);

            // Select job cards, enqueue detail links
            const jobLinks = [];
            $('a.tapItem[href]').each((_, el) => {
                const href = $(el).attr('href');
                if (!href) return;
                // normalize to absolute
                const url = new URL(href, BASE_URL).href;
                // Avoid sponsored 'pagead' if desired; still enqueue as fallback
                jobLinks.push(url);
            });

            for (const url of jobLinks) {
                // Stop if max_items reached
                if (itemCount >= max_items) break;
                await crawler.addRequests([{
                    url,
                    userData: { label: 'DETAIL' },
                }]);
            }

            // Pagination: find "Next" link
            if (pageCount < max_pages && itemCount < max_items) {
                const nextSel = 'a[aria-label="Next"], a[data-testid="pagination-page-next"]';
                const nextHref = $(nextSel).attr('href');
                if (nextHref) {
                    const nextUrl = new URL(nextHref, BASE_URL).href;
                    await crawler.addRequests([{ url: nextUrl, userData: { label: 'LIST' } }]);
                }
            }
        } else {
            // DETAIL page
            if (!$) {
                // fetch with gotScraping and load to cheerio
                const resp = await gotScraping({ url: request.url });
                $ = cheerio.load(resp.body);
            }

            // Title
            const title = pickText($, [
                'h1[data-testid="jobsearch-JobTitle"]',
                'h1.jobsearch-JobInfoHeader-title',
                'h1',
            ]);

            // Company & location
            // Indeed uses a metadata header with company and location
            const company = pickText($, [
                '[data-company-name] a',
                '[data-company-name]',
                '.jobsearch-CompanyInfoWithoutHeaderImage div a',
                '.jobsearch-CompanyInfoWithoutHeaderImage div',
                'div[data-testid="inlineHeader-companyName"]',
            ]);

            const locationText = pickText($, [
                'div[data-testid="inlineHeader-companyLocation"]',
                '.jobsearch-CompanyInfoWithoutHeaderImage div:has(svg) + div',
                '.jobsearch-CompanyInfoWithoutHeaderImage > div:last-child',
            ]);

            // Description
            const descriptionHtml = pickHtml($, [
                '#jobDescriptionText',
                'div#jobDescriptionText',
                'section#jobDescriptionText',
            ]);
            const descriptionText = descriptionHtml
                ? norm($( '#jobDescriptionText' ).text())
                : pickText($, ['#jobDescriptionText', 'section#jobDescriptionText', 'article']);

            // Posted time (e.g., "Just posted", "Posted 2 days ago")
            const jobPosted = pickText($, [
                'div.jobsearch-JobMetadataFooter > div:last-child',
                'div[data-testid="jobsearch-JobMetadataFooter"] span:contains("Posted")',
                'span:contains("Just posted")',
                'span:contains("Today")',
                'span:contains("Active")',
            ]) || pickText($, ['span:matches(/Posted|Just posted|Today|Active/i)']);

            // Job types (e.g., Full-time, Contract)
            // Often under a "Job Type" row inside #jobDetails or a definition list
            const jobTypes = (() => {
                const types = new Set();
                // Newer layout
                $('div[data-testid="job-details"] div:contains("Job type")')
                    .next()
                    .find('li')
                    .each((_, li) => types.add(norm($(li).text())));
                // Fallback older layout
                $('#jobDetails ~ div ul li:contains("Job Type")')
                    .next('li')
                    .each((_, li) => types.add(norm($(li).text())));
                // Generic tokens seen on Indeed
                $('li').each((_, li) => {
                    const t = norm($(li).text());
                    if (/full[-\s]?time|part[-\s]?time|contract|temporary|internship|commission|per[-\s]?diem|apprenticeship|remote/i.test(t)) {
                        types.add(t);
                    }
                });
                return Array.from(types).filter(Boolean);
            })();

            const out = {
                title: title || null,
                company: company || null,
                location: locationText || null,
                description_text: descriptionText || null,
                description_html: descriptionHtml || null,
                job_posted: jobPosted || null,
                job_types: jobTypes.length ? jobTypes : null,
                url: request.url,
            };

            await Dataset.pushData(out);
            itemCount += 1;
            log.debug(`Saved item #${itemCount}: ${out.title || out.url}`);
        }
    },
    failedRequestHandler({ request, log }) {
        log.error(`Request failed after retries: ${request.url}`);
    },
});

await crawler.run([{ url: startUrl.href, userData: { label: 'LIST' } }]);

log.info(`Done. Saved ${itemCount} items across ${pageCount} list page(s).`);
await Actor.exit();
