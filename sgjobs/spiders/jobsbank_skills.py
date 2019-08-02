# -*- coding: utf-8 -*-
import json
import scrapy


class JobsbankSpider(scrapy.Spider):
    name = 'jobsbank-skills'
    allowed_domains = ['mycareersfuture.sg']
    base_url = 'https://api.mycareersfuture.sg/v2/skills/{}'
    last_skill = 9993
    current_skill = 1
    start_urls = [base_url.format(current_skill)]    


    def parse(self, response):
        data = json.loads(response.body)        

        # Yield every result on the page
        if 'id' in data:
            yield {'id': data['id'], 'skill': data['skill']}

        # Yield next result
        if self.current_skill < self.last_skill:
            self.current_skill += 1
            yield scrapy.Request(self.base_url.format(self.current_skill), callback=self.parse)
