# -*- coding: utf-8 -*-
import json
import scrapy


class JobscentralSpider(scrapy.Spider):
    name = 'jobscentral'
    allowed_domains = ['jobscentral.com.sg']
    base_url = 'https://jobscentral.com.sg/api/jobs/search?page={}&perPage={}'
    job_url = 'https://jobscentral.com.sg/api/jobs/{}'
    default_max_limit = -1
    results_per_page = 1000
    current_page = 1
    start_urls = [base_url.format(current_page, results_per_page)]
    crawl_page_limit = default_max_limit


    def parse(self, response):
        data = json.loads(response.body)        

        # Yield every result on the page
        if data:
            for entry in data:
                yield scrapy.Request(self.job_url.format(entry['id']), callback=self.parse_posting)

            #if JobscentralSpider.default_max_limit < 0 or JobscentralSpider.current_page <= self.default_max_limit:
            yield scrapy.Request(self.base_url.format(JobscentralSpider.current_page, 
                                                      JobscentralSpider.results_per_page), 
                                                callback=self.parse)


    def parse_posting(self, response):
        data = json.loads(response.body)
        yield data
