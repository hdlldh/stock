# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class InvestItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass

#class InvestSummary(scrapy.Item):
#	Symbol = scrapy.Field()
#    Date = scrapy.Field()


class ProfileItem(scrapy.Item):
    Symbol = scrapy.Field()
    EventDate = scrapy.Field()
    Timestamp = scrapy.Field()
    Sector = scrapy.Field()
    Industry = scrapy.Field()
    Full_Time_Employees = scrapy.Field()
