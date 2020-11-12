# -*- coding: utf-8 -*-
import json
import re
import scrapy


class JobsbankSpider(scrapy.Spider):
    name = 'jobsbank'
    allowed_domains = ['mycareersfuture.sg', 'mycareersfuture.gov.sg']
    base_url = 'https://api.mycareersfuture.sg/v2/jobs?limit={}&page={}'
    default_max_limit = -1
    results_per_page = 10
    parsed_pages = 0
    start_urls = [base_url.format(results_per_page, 1)]
    crawl_page_limit = 3


    def parse(self, response):
        data = json.loads(response.body)
        results = data.get('results', [])

        # Yield every result on the page
        for entry in results:
            yield entry

        self.parsed_pages += 1

        links = data.get('_links', None)
        if not links:
            return

        if not (links['self']['href'] == links['last']['href']):                
            next_page_nums = re.findall(r"page=([0-9]+)&", links['next']['href'])            
            if self.crawl_page_limit < 0 or (next_page_nums and int(next_page_nums[0]) <= self.crawl_page_limit):                
                yield scrapy.Request(links['next']['href'], callback=self.parse)
