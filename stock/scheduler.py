import pandas as pd
import datetime as dt
from scrapy.crawler import CrawlerProcess
from pandas.tseries.holiday import USFederalHolidayCalendar


FedCal = USFederalHolidayCalendar()
crnt_time = dt.datetime.now()
crnt_date = dt.datetime(crnt_time.year,crnt_time.month,crnt_time.day)
crnt_year = crnt_time.year

## every business days ##
BusDays = pd.date_range('%s'%crnt_year,'%s'%(crnt_year+1),freq='B')
## every Saturday
SatDays = pd.date_range('%s'%crnt_year,'%s'%(crnt_year+1),freq='W-SAT')
## every Sunday
SunDays = pd.date_range('%s'%crnt_year,'%s'%(crnt_year+1),freq='W-SUN')
## every 1st business days
BmsDays = pd.date_range('%s'%crnt_year,'%s'%(crnt_year+1),freq='BMS')
## One day before 1st business days
BmsDays_1 = BmsDays.shift(-1)

settings ={}
settings['BOT_NAME'] = 'stock'
settings['SPIDER_MODULES'] = ['stock.spiders']
settings['NEWSPIDER_MODULE'] = 'stock.spiders'
#settings['USER_AGENT'] = None
settings['ROBOTSTXT_OBEY'] = False
settings['DOWNLOAD_DELAY'] = 1
settings['ITEM_PIPELINES'] = {
        'stock.pipelines.SQLiteStoreItemPipeline': 300,
        }
settings['AUTOTHROTTLE_ENABLED'] = True
settings['AUTOTHROTTLE_START_DELAY'] = 5
settings['AUTOTHROTTLE_MAX_DELAY'] = 60
settings['AUTOTHROTTLE_TARGET_CONCURRENCY'] = 1.0
settings['DOWNLOADER_MIDDLEWARES'] = {
        'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware':None,
        'stock.rotate_useragent.RotateUserAgentMiddleware' :400
        }



process = CrawlerProcess(settings)
start_signal = 0 

if crnt_date in BusDays and crnt_date not in FedCal.holidays():
    print('Run Yahoo stock summary')
    process.crawl("yahoo_stock_summary")
    start_signal = 1


if crnt_date in SatDays:
    print('Run Yahoo stock statistics')
    process.crawl("yahoo_stock_statistics")
    start_signal = 1
    


if crnt_date in BmsDays_1:
    print('Run Yahoo stock profile')
    process.crawl("yahoo_stock_profile")
    start_signal = 1


if start_signal: process.start()


