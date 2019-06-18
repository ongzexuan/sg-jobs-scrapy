# -*- coding: utf-8 -*-
import json
import re
import scrapy

from bs4 import BeautifulSoup


class FastjobsSpider(scrapy.Spider):
    name = 'fastjobs'
    allowed_domains = ['fastjobs.sg']
    base_url = 'https://www.fastjobs.sg/singapore-jobs/en/all-categories-jobs/page-{}/'
    filter_prefix = 'https://www.fastjobs.sg/singapore-job-ad'
    default_max_limit = -1
    start_urls = [base_url.format(1)]
    crawl_page_limit = default_max_limit    

    def parse(self, response):

        soup = BeautifulSoup(response.body, 'lxml')
        results = soup.find_all('a', href=True)
        results = [r for r in results if r['href'].startswith(self.filter_prefix)]

        if results:
            for r in results:
                yield scrapy.Request(r['href'], callback=self.parse_posting)

            # Get next page
            next_pages = soup.find_all('li', class_='next')
            if next_pages and next_pages[0].a:
                yield scrapy.Request(next_pages[0].a['href'], callback=self.parse)


    def parse_posting(self, response):

        soup = BeautifulSoup(response.body, 'lxml')

        # Extract data from the application/ld+json field
        data = json.loads(soup.find('script', type='application/ld+json').text, strict=False)        

        # Add extra information from the residue on the page
        res = re.findall(r'UEN:.+?<br/>', str(response.body))
        if res:
            uen = res[0][4: res[0].find('<')].strip()
            data['uen'] = uen
        res = re.findall(r'pubdttme\":\".+?\"', str(response.body))
        if res:
            posted_on = res[0][res[0].find(':') + 2: res[0].rfind('"')].strip()
            data['posted_on'] = posted_on
        res = re.findall(r'Expiring On .+?<br/>', str(response.body))
        if res:
            expiring_on = res[0][11: res[0].find('<')].strip()
            data['expiring_on'] = expiring_on
        res = re.findall(r'minsalary\":\".+?\"', str(response.body))
        if res:
            min_salary = res[0][res[0].find(':') + 2: res[0].rfind('"')].strip()
            if min_salary.endswith('k'):
                min_salary = float(min_salary[0:-1]) * 1000
            else:
                min_salary = float(min_salary)
            data['min_salary'] = min_salary

        yield data

