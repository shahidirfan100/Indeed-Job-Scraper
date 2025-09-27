import { Actor } from 'apify';
import { CheerioCrawler, Dataset, log } from 'crawlee';
import { gotScraping } from 'got-scraping';

const BASE_URL = 'https://www.indeed.com';

const datePostedMap = {
    '24h': '1',
    '7d': '7',
    '30d': '30',
};

await Actor.init();

const input = await Actor.getInput() || {};
const { 
    keyword, 
    location, 
    posted_date = 'anytime', 
    results_wanted = 100, 
    proxyConfiguration 
} = input;

if (!keyword) {
    throw new Error('Input "keyword" is required.');
}

let jobCount = 0;

const proxyConfig = await Actor.createProxyConfiguration(proxyConfiguration);

const crawler = new CheerioCrawler({
    proxyConfiguration: proxyConfig,
    maxRequestsPerCrawl: results_wanted + 50, // A bit of buffer
    requestHandler: async ({ $, request, enqueueLinks }) => {
        const { userData: { label } } = request;

        if (label === 'LIST') {
            log.info(`Scraping list page: ${request.url}`);

            const jobCards = $('.jobsearch-ResultsList .job_seen_beacon');

            for (const jobCard of jobCards) {
                if (jobCount >= results_wanted) break;

                const title = $(jobCard).find('h2.jobTitle a span').text().trim();
                const company = $(jobCard).find('span.companyName').text().trim();
                const jobLocation = $(jobCard).find('div.companyLocation').text().trim();
                const datePosted = $(jobCard).find('span.date').text().trim();
                const jobLink = $(jobCard).find('h2.jobTitle a').attr('href');

                if (jobLink) {
                    const absoluteJobLink = new URL(jobLink, BASE_URL).href;
                    await crawler.addRequests([{
                        url: absoluteJobLink,
                        userData: { 
                            label: 'DETAIL',
                            jobData: { title, company, location: jobLocation, date_posted: datePosted, url: absoluteJobLink }
                        }
                    }]);
                    jobCount++;
                }
            }

            // Pagination
            if (jobCount < results_wanted) {
                const nextPageLink = $('a[data-testid="pagination-page-next"]').attr('href');
                if (nextPageLink) {
                    const absoluteNextPageLink = new URL(nextPageLink, BASE_URL).href;
                    await crawler.addRequests([{
                        url: absoluteNextPageLink,
                        userData: { label: 'LIST' }
                    }]);
                }
            }
        } else if (label === 'DETAIL') {
            log.info(`Scraping detail page: ${request.url}`);
            const { jobData } = request.userData;

            const descriptionHtml = $('#jobDescriptionText').html();
            const descriptionText = $('#jobDescriptionText').text().trim();

            await Dataset.pushData({
                ...jobData,
                description_html: descriptionHtml,
                description_text: descriptionText,
            });
        }
    },
});

const startUrl = new URL('/jobs', BASE_URL);
startUrl.searchParams.set('q', keyword);
if (location) {
    startUrl.searchParams.set('l', location);
}
if (posted_date !== 'anytime' && datePostedMap[posted_date]) {
    startUrl.searchParams.set('fromage', datePostedMap[posted_date]);
}

log.info(`Starting crawl with URL: ${startUrl.href}`);
await crawler.run([{ url: startUrl.href, userData: { label: 'LIST' } }]);

await Actor.exit();