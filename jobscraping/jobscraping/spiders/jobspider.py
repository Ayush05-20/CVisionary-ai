import scrapy
from jobscraping.items import JobItem

class JobspiderSpider(scrapy.Spider):
    name = "jobspider"
    allowed_domains = ["merojob.com"]
    start_urls = ["https://merojob.com/search/?"]
    max_pages = 10  # limit to 10 pages
    current_page = 1
    
    custom_settings = {
        'FEEDS' :
            {
                "cleaned.json" : {'format' : 'json', "overwrite" : True, 'encoding': 'utf8'}
            }
    }

    def parse(self, response):
        merojob = response.css('div.card.hover-shadow')
        
        for job in merojob:
            relative_url = job.css('h1.text-primary.font-weight-bold.media-heading.h4 a::attr(href)').get()
            
            job_url = 'https://merojob.com' + relative_url
            yield response.follow(job_url, callback=self.parse_job_details)
            
        next_page = response.css('a.pagination-next.page-link::attr(href)').get()
        
        if self.current_page < self.max_pages:
            next_page = response.css('a.pagination-next.page-link::attr(href)').get()
            if next_page:
                self.current_page += 1
                yield response.follow(next_page, callback=self.parse)
            
    def parse_job_details(self, response):
        job_item = JobItem()
        job_item["url"]= response.url
        job_item["title"]= response.css('h1[itemprop="title"]::text').get().replace('\n', '').strip() or ''
        job_item["job_cat"]= response.css('td a::text').get() or ''
        job_item["location"]= response.css('td span.clearfix::text').get() or ''
        job_item["company"]= response.css('span[itemprop="name"]::text').get() or ''
        job_item["education"]= response.css('td span[itemprop="educationRequirements"]::text').get() or ''
        job_item["experience"]= response.css('td span[itemprop="experienceRequirements"]::text').get() or ''
        job_item["skills"] = response.css('span[itemprop="skills"] span.badge::text').getall() or ''
        job_item['general_requirements'] = response.xpath('//div[contains(@class, "card-text p-2")]//ul[1]/li//text()').getall()
        job_item['specific_requirements'] = response.xpath('//div[contains(@class, "card-text p-2")]//ul[2]/li//text()').getall()
        job_item["dis"] = response.xpath('//div[@class="card-text p-2"][@itemprop="description"]//p[1]/span/text()').getall() or ''
        job_item['responsibilities'] = response.xpath('//div[@class="card-text p-2" and @itemprop="description"]/p/following-sibling::ul[1]/li//text()').getall() or ''
        yield job_item