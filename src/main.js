// src/main.js — Indeed scraper aligned to your Workable actor’s contract, with anti-403 fallbacks
import { Actor } from 'apify';
import { CheerioCrawler, Dataset, log, sleep } from 'crawlee';
import * as cheerio from 'cheerio';

// ----------------- CONSTANTS -----------------
const DESKTOP_HOST = 'https://www.indeed.com';
const MOBILE_HOST  = 'https://m.indeed.com';
const DATE_MAP     = { '24h': '1', '7d': '7', '30d': '30' };

// ----------------- HELPERS -----------------
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

// Extract jk from any href with /viewjob?jk=...
const jkFromHref = (href) => {
    try {
        const u = new URL(href, DESKTOP_HOST);
        const jk = u.searchParams.get('jk');
        return jk || null;
    } catch {
        return null;
    }
};

// Read "Posted X days ago / Just posted / Today" ONLY from footer to avoid CSS dumps
const extractPosted = ($) => {
    const footerTxt = norm($('div.jobsearch-JobMetadataFooter, [data-testid="jobsearch-JobMetadataFooter"]').text());
    if (!footerTxt) return null;

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
    const m2 = footerTxt.match(/Posted[^|]+/i);
    return m2 ? m2[0].trim() : null;
};

// Extract all required fields from a DETAIL page
const extractDetail = ($, url, seed) => {
    stripDomNoise($);

    const title = pickText($, [
        'h1[data-testid="jobsearch-JobTitle"]',
        'h1.jobsearch-JobInfoHeader-title',
        'h1',
        // Mobile fallback
        'h1.jobsearch-JobInfoHeader-title-container',
    ]) ?? seed?.title ?? null;

    const company = pickText($, [
        '[data-company-name] a',
        '[data-company-name]',
        'div[data-testid="inlineHeader-companyName"]',
        '.jobsearch-CompanyInfoWithoutHeaderImage div a',
        '.jobsearch-CompanyInfoWithoutHeaderImage div',
        // Mobile fallback
        'div.jobsearch-CompanyInfoContainer a',
        'div.jobsearch-CompanyInfoContainer',
    ]) ?? seed?.company ?? null;

    const location = pickText($, [
        'div[data-testid="inlineHeader-companyLocation"]',
        '.jobsearch-CompanyInfoWithoutHeaderImage > div:last-child',
        // Mobile fallback
        'div.jobsearch-CompanyInfoContainer ~ div',
    ]) ?? seed?.location ?? null;

    const description_html = pickHtml($, [
        '#jobDescriptionText',
        'div#jobDescriptionText',
        'section#jobDescriptionText',
        // Mobile fallback
        'div#jobDescriptionTextContainer',
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
        description_text = norm($('#jobDescriptionText, section#jobDescriptionText, article, #jobDescriptionTextContainer').text()) || null;
    }

    const date_posted = extractPosted($) ?? seed?.date_posted ?? null;

    const job_types = (() => {
        const types = new Set();
        $('div[data-testid="job-details"] div:contains("Job type")')
            .next()
            .find('li')
            .each((_, li) => types.add(norm($(li).text())));
        // Mobile / generic fallback: scan for tokens
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

// ----------------- MAIN -----------------
await Actor.init();

const input = (await Actor.getInput()) || {};
const {
    keyword = '',
    location = '',
    postedWithin = '7d',   // '24h' | '7d' | '30d'
    results_wanted = 200,
    maxConcurrency = 5,
    proxyConfiguration = null,
} = input;

const proxyConfig = await Actor.createProxyConfiguration(proxyConfiguration);

// Build desktop & mobile LIST seeds up-front (so a desktop 403 doesn't kill the run)
const fromage = DATE_MAP[postedWithin];
const makeListUrl = (host) => {
    const u = new URL('/jobs', host);
    if (keyword)  u.searchParams.set('q', keyword);
    if (location) u.searchParams.set('l', location);
    if (fromage)  u.searchParams.set('fromage', fromage);
    return u.toString();
};
const listSeeds = [
    { url: makeListUrl(DESKTOP_HOST), label: 'LIST_DESKTOP' },
    { url: makeListUrl(MOBILE_HOST),  label: 'LIST_MOBILE'  },
];

const state = { enqueued: 0, saved: 0, pages: { desktop: 0, mobile: 0 } };

const crawler = new CheerioCrawler({
    proxyConfiguration: proxyConfig,
    maxConcurrency,
    useSessionPool: true,
    persistCookiesPerSession: true,
    maxRequestRetries: 3,
    requestHandlerTimeoutSecs: 60,
    navigationTimeoutSecs: 60,

    // Crawlee 3.x-safe: set headers and pacing here
    preNavigationHooks: [
        async (ctx, gotOptions) => {
            const { session, request } = ctx;
            const ua = session?.userData?.ua || randomDesktopUA();
            if (session && !session.userData.ua) session.userData.ua = ua;

            // Desktop-like headers even on mobile host (helps consistency)
            gotOptions.headers = {
                ...(gotOptions.headers || {}),
                'User-Agent': ua,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Cache-Control': 'no-cache',
                'Pragma': 'no-cache',
                'Upgrade-Insecure-Requests': '1',
                'Referer': 'https://www.google.com/',
                // Extra hints (harmless but helpful)
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Dest': 'document',
            };

            // Random jitter
            await sleep(800 + Math.floor(Math.random() * 1400));
        },
    ],

    async requestHandler({ request, $, addRequests, response, session, log }) {
        const label = request.userData?.label;
        const status = response?.statusCode ?? response?.status;

        // Early block detection
        if (status === 403 || status === 429) {
            session?.markBad();
            throw new Error(`Blocked ${status}`);
        }

        // ----------------- LIST (desktop or mobile) -----------------
        if (label === 'LIST_DESKTOP' || label === 'LIST_MOBILE') {
            const isMobile = label === 'LIST_MOBILE';
            if (isMobile) state.pages.mobile += 1;
            else state.pages.desktop += 1;

            log.info(`${label} page #${isMobile ? state.pages.mobile : state.pages.desktop}: ${request.url}`);

            // Find detail links — ONLY via /viewjob?jk=... to avoid /rc/clk & /pagead/clk 403s
            const detailReqs = [];
            const host = isMobile ? MOBILE_HOST : DESKTOP_HOST;

            // 1) Prefer explicit viewjob links (works on both sites)
            $('a[href*="/viewjob?jk="]').each((_, el) => {
                if (state.enqueued >= results_wanted) return;
                const href = $(el).attr('href');
                const jk = jkFromHref(href);
                if (!jk) return;

                const detailUrl = `${DESKTOP_HOST}/viewjob?jk=${jk}`; // request desktop first
                const seed = {};

                const title = norm($(el).find('h2, h3, .jobTitle').first().text()) || null;
                if (title) seed.title = title;

                const company = norm($(el).closest('[data-company-name]').attr('data-company-name') || $(el).closest('.job_seen_beacon').find('[data-company-name], .companyName').first().text() || '');
                if (company) seed.company = company;

                const loc = norm($(el).closest('.job_seen_beacon').find('[data-testid="text-location"], .companyLocation').first().text() || '');
                if (loc) seed.location = loc;

                const posted = norm($(el).closest('.job_seen_beacon, li, div').find('span:contains("Posted"), span:contains("Just posted"), span:contains("Today")').first().text() || '');
                if (posted) seed.date_posted = posted;

                detailReqs.push({ url: detailUrl, userData: { label: 'DETAIL', ...seed } });
                state.enqueued += 1;
            });

            // 2) Also try to mine jk from card containers if present
            $('[data-jk]').each((_, el) => {
                if (state.enqueued >= results_wanted) return;
                const jk = $(el).attr('data-jk');
                if (!jk) return;
                const detailUrl = `${DESKTOP_HOST}/viewjob?jk=${jk}`;
                detailReqs.push({ url: detailUrl, userData: { label: 'DETAIL' } });
                state.enqueued += 1;
            });

            if (detailReqs.length) await addRequests(detailReqs);
            if (state.enqueued >= results_wanted) return;

            // Pagination
            let nextUrl;
            const nextHref = $('a[aria-label="Next"], a[data-testid="pagination-page-next"]').attr('href');
            if (nextHref) {
                nextUrl = new URL(nextHref, host).href;
            } else {
                const u = new URL(request.url);
                const current = parseInt(u.searchParams.get('start') || '0', 10);
                u.searchParams.set('start', String(current + 10));
                nextUrl = u.href;
            }
            await addRequests([{ url: nextUrl, userData: { label } }]);

            return;
        }

        // ----------------- DETAIL -----------------
        let $$ = $;
        if (!$$) {
            // Fallback refetch with same session headers
            const resp = await this.sendRequest({ url: request.url, responseType: 'text' });
            if (resp.statusCode === 403 || resp.statusCode === 429) {
                session?.markBad();
                throw new Error(`Blocked ${resp.statusCode} on detail`);
            }
            $$ = cheerio.load(resp.body);
        }

        // If the desktop detail page looks blocked/empty, try mobile viewjob as a fallback
        const looksBlocked = !$$('#jobDescriptionText, #jobDescriptionTextContainer').length && /verify|robot|captcha|unusual traffic/i.test($$.root().text());
        if (looksBlocked || ($$.root().text().trim().length < 50)) {
            try {
                const jk = jkFromHref(request.url) || request.url.match(/[?&]jk=([a-z0-9]+)/i)?.[1];
                if (jk) {
                    const altUrl = `${MOBILE_HOST}/viewjob?jk=${jk}`;
                    const resp = await this.sendRequest({ url: altUrl, responseType: 'text' });
                    if (resp.statusCode !== 403 && resp.statusCode !== 429) {
                        $$ = cheerio.load(resp.body);
                    }
                }
            } catch { /* ignore */ }
        }

        const result = extractDetail($$, request.url, request.userData);
        await Dataset.pushData(result);
        state.saved += 1;
        log.debug(`Saved: ${result.title || '(untitled)'} — ${result.company || ''}`);
    },

    failedRequestHandler: ({ request, error, session }) => {
        // Don’t stop the whole run; mark session bad and continue
        session?.markBad?.();
        log.error(`Failed after retries: ${request.url} — ${error?.message}`);
    },
});

// ---------- Kickoff with BOTH list seeds (desktop + mobile) ----------
await crawler.addRequests(listSeeds.map(s => ({ url: s.url, userData: { label: s.label } })));
await crawler.run();

log.info(`Done. Enqueued ${state.enqueued}, saved ${state.saved}, pages D:${state.pages.desktop} M:${state.pages.mobile}.`);
await Actor.exit();
