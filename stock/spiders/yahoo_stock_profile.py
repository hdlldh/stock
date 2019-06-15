# -*- coding: utf-8 -*-
from __future__ import absolute_import
import scrapy
import datetime as dt
import pandas as pd
import os


class YahooStockProfileSpider(scrapy.Spider):
    name = 'yahoo_stock_profile'
    allowed_domains = ['yahoo.com']
    
    def __init__(self):
        cur_path, _ = os.path.split(__file__)
        root_path = os.path.dirname(cur_path)
        symbol_file = os.path.join(root_path,'tickers/test.csv')
        #symbol_file = './stock/tickers/test.csv'
        symbols = pd.read_csv(symbol_file,dtype=object)
        symbol_list = list(symbols['Symbol'])

        # example url: 'https://finance.yahoo.com/quote/T/profile?p=T'
        url_base = 'https://finance.yahoo.com/quote/%(sym)s/profile?p=%(sym)s'
        self.start_urls = [url_base%{'sym':symbol} for symbol in symbol_list]
        self.EventDate = dt.datetime.now().strftime('%Y-%m-%d')
        self.allowed_fields = ['Sector','Industry','Full Time Employees']

    def parse(self, response):
        symbol = response.url.split('=')[-1]
        item = dict()
        item['Symbol'] = symbol

        xpath = dict()
        xpath['Company'] = '//*[@id="Col1-0-Profile-Proxy"]/section/div[1]/div/h3//text()'
        xpath['Sector'] = '//*[@id="Col1-0-Profile-Proxy"]/section/div[1]/div/div/p[2]/span[2]//text()'
        xpath['Industry'] = '//*[@id="Col1-0-Profile-Proxy"]/section/div[1]/div/div/p[2]/span[4]//text()'
        xpath['Employees'] = '//*[@id="Col1-0-Profile-Proxy"]/section/div[1]/div/div/p[2]/span[6]/span//text()'

        for k, v in xpath.items():
            item[k] = response.xpath(v).extract_first()

        item['EventDate'] = self.EventDate
        item['Timestamp'] = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        return item
