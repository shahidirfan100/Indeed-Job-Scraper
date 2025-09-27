# Indeed Jobs Scraper

This Apify actor scrapes job listings from Indeed.com using HTTP requests and Cheerio for parsing. It's a fast and lightweight scraper that can be run directly on the Apify platform.

## Features

- Scrapes job titles, companies, locations, and posted dates.
- Extracts full job descriptions in both HTML and plain text.
- Supports filtering by keyword, location, and date posted.
- Paginates through search results to reach the desired number of jobs.
- Uses `got-scraping` for robust HTTP requests.

## Input

The actor takes the following input:

- **keyword**: The search term for jobs (e.g., 'Software Engineer').
- **location**: The geographic location to search for jobs (e.g., 'New York, NY', 'Remote').
- **posted_date**: Filter jobs by when they were posted ('24h', '7d', '30d', 'anytime').
- **results_wanted**: The maximum number of jobs to fetch.

## Output

The actor saves the scraped job data to the Apify dataset in the following format:

```json
{
  "title": "...",
  "company": "...",
  "location": "...",
  "date_posted": "...",
  "description_html": "<p>...</p>",
  "description_text": "Plain text job description",
  "url": "..."
}
```