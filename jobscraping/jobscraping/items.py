# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class JobscrapingItem(scrapy.Item):
    # define the fields for your item here like:
    name = scrapy.Field()
    pass

class JobItem(scrapy.Item):
    url = scrapy.Field()
    title = scrapy.Field()
    job_cat = scrapy.Field()
    location = scrapy.Field()
    company = scrapy.Field()
    education = scrapy.Field()
    experience = scrapy.Field()
    skills = scrapy.Field()
    general_requirements = scrapy.Field()
    specific_requirements = scrapy.Field()
    dis = scrapy.Field()
    responsibilities = scrapy.Field()