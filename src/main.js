// src/main.js — Indeed version aligned to your Workable actor’s contract & style
import { Actor } from 'apify';
import { CheerioCrawler, Dataset, log, sleep } from 'crawlee';
import * as cheerio from 'cheerio';

// -------- Config mirrors your project --------
const BASE_URL = 'https://www.indeed.com';
const dateMap = { '24h': '1', '7d': '7', '30d': '30' }; // maps postedWithin -> fromage

// -------- Helpers (same approach as your style) --------
const norm = (t) => (t || '').replace(/\s+/g, ' ').trim();

function pickText($, selectors) {
    for (const sel of selectors) {
        const txt = $(sel).first().text();
        if (txt && norm(txt)) return norm(txt);
    }
    return null;
}

function pickHtml($, selectors) {
    for (const sel of selectors) {
        const html = $(sel).first().html();
        if (html && norm(html)) return html.trim();
    }
    return null;
}

function randomDesktopUA() {
    const uas = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
    ];
    return uas[Math.floor(Math.random() * uas.length)];
}

// Extract fields from a job detail page to your expected schema
function extractDetail($, url, seed) {
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
            description_text = $$.text().replace(/\s+\n/g, '\n').replace(/\s{2,}/g, ' ').trim() || null;
        } catch {
            description_text = $('#jobDescriptionText').text().trim() || null;
        }
    } else {
        description_text = $('#jobDescriptionText, section#jobDescriptionText, article').text().trim() || null;
    }

    // date_posted: keep the same name as your Workable pipeline expects
    const date_posted = pickText($, [
        'div.jobsearch-JobMetadataFooter > div:last-child',
        'div[data-testid="jobsearch-JobMetadataFooter"] span:contains("Posted")',
        'span:contains("Just posted")',
        'span:contains("Today")',
        'span:contains("Active")',
    ]) ?? seed?.date_posted ?? null;

    // job_types from detail page
    const job_types = (() => {
        const types = new Set();
        $('div[data-testid="job-details"] div:contains("Job type")')
            .next()
            .find('li')
            .each((_, li) => types.add(norm($(li).text())));
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
}

await Actor.init();

// -------- Inputs: keep EXACTLY your working contract --------
const input = (await Actor.getInput()) || {};
const {
    keyword = '',
    location = '',
    postedWithin = '7d',     // '24h' | '7d' | '30d'
    results_wanted = 200,    // cap results like your Workable actor
    maxConcurrency = 5,      // keep same control as your working file
    proxyConfiguration = null,
} = input;

const proxyConfig = await Actor.createProxyConfiguration(proxyConfiguration);

// Build Indeed search URL using same variables
const startUrl = new URL('/jobs', BASE_URL);
if (keyword) startUrl.searchParams.set('q', keyword);
if (location) startUrl.searchParams.set('l', location);
const fromage = dateMap[postedWithin];
if (fromage) startUrl.searchParams.set('fromage', fromage);

const state = { collectedCount: 0, pageCount: 0 };

const crawler = new CheerioCrawler({
    proxyConfiguration: proxyConfig,
    maxConcurrency,
    useSessionPool: true,
    persistCookiesPerSession: true,
    requestHandlerTimeoutSecs: 60,
    navigationTimeoutSecs: 60,
    maxRequestRetries: 3,

    // Crawlee 3.x-safe: set headers and add jitter here
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

            // gentle jitter like your style; use crawlee.sleep
            const ms = 800 + Math.floor(Math.random() * 1400);
            await sleep(ms);
        },
    ],

    async requestHandler({ request, $, addRequests, response, session, log }) {
        const { label } = request.userData || {};
        const status = response?.statusCode ?? response?.status;
        if (status === 403 || status === 429) {
            session?.markBad();
            throw new Error(`Blocked ${status}`);
        }
        if ($) {
            const text = norm($.root().text().slice(0, 2000)).toLowerCase();
            if (/verify|robot|unusual traffic|are you a human|captcha/.test(text)) {
                session?.markBad();
                throw new Error('Bot/verification page');
            }
        }

        if (label === 'LIST') {
            state.pageCount += 1;
            log.info(`LIST page ${state.pageCount}: ${request.url}`);

            // Collect detail links from cards
            const detailUrls = [];
            $('a.tapItem[href], a[data-jk][href]').each((_, el) => {
                const href = $(el).attr('href');
                if (!href) return;
                if (/^https?:\/\/(www\.)?google\./i.test(href)) return; // skip ad redirects
                const abs = new URL(href, BASE_URL).href;
                detailUrls.push(abs);
            });

            for (const url of detailUrls) {
                if (state.collectedCount >= results_wanted) break;
                await addRequests([{ url, userData: { label: 'DETAIL' } }]);
                state.collectedCount += 1;
            }

            // Stop if we hit the cap
            if (state.collectedCount >= results_wanted) return;

            // Pagination: prefer a Next link, else increment ?start= by 10
            const nextSel = 'a[aria-label="Next"], a[data-testid="pagination-page-next"]';
            const nextHref = $(nextSel).attr('href');
            let nextUrl = null;
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
            // DETAIL page
            let $$ = $;
            if (!$$) {
                // Fallback: refetch HTML via Crawlee client (inherits headers/session)
                const resp = await this.sendRequest({ url: request.url, responseType: 'text' });
                if (resp.statusCode === 403 || resp.statusCode === 429) {
                    session?.markBad();
                    throw new Error(`Blocked ${resp.statusCode} on detail`);
                }
                $$ = cheerio.load(resp.body);
            }

            const result = extractDetail($$, request.url, request.userData);
            await Dataset.pushData(result);
            log.debug(`Saved: ${result.title || '(untitled)'} — ${result.company || ''}`);
        }
    },

    async failedRequestHandler({ request }) {
        log.warning(`Request failed and reached max retries: ${request.url}`);
    },
});

// Kickoff (mirror your style)
await crawler.addRequests([{ url: startUrl.toString(), userData: { label: 'LIST' } }]);
await crawler.run();

log.info(`Scraper finished. Collected ${state.collectedCount} items across ${state.pageCount} list page(s).`);
await Actor.exit();
