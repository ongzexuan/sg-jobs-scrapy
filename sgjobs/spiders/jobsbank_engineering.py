# -*- coding: utf-8 -*-
import json
import scrapy


class JobsbankSpider(scrapy.Spider):
    name = 'jobsbank-engineering'
    allowed_domains = ['mycareersfuture.sg']
    base_url = 'https://api.mycareersfuture.sg/v2/jobs?limit={}&page={}&category=Engineering'
    default_max_limit = -1
    results_per_page = 100
    start_urls = [base_url.format(results_per_page, 1)]
    crawl_page_limit = default_max_limit


    def parse(self, response):
        data = json.loads(response.body)
        results = data.get('results', [])

        # Yield every result on the page
        for entry in results:
            categories = entry.get('categories', None)            
            if categories:
                for category in categories:
                    category_id = category['id']
                    if category_id == 11:
                        yield entry
                        break

        links = data.get('_links', None)
        if links:
            if not (links['self']['href'] == links['last']['href']):
                # Don't crawl beyond max limit
                next_page_num = int(links['next']['href'][-1])
                if self.crawl_page_limit <= 0 or next_page_num <= self.crawl_page_limit:

                    # After that, yield the next page's results
                    yield scrapy.Request(links['next']['href'], callback=self.parse)
