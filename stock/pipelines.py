# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html



import sqlite3
import datetime as dt
import os

class InvestPipeline(object):
    def process_item(self, item, spider):
        return item

 
class SQLiteStoreItemPipeline(object):
    
 
    def __init__(self):
        #abs_path = os.path.dirname(mylib.__file__)
        abs_path = os.path.dirname(os.path.realpath(__file__))
        self.file_name = os.path.join(abs_path,'data/yahoo_stocks_%s.db'%(dt.datetime.now().strftime('%Y%m%d')))
        #self.file_name = './test.db'
        self.conn = sqlite3.connect(self.file_name)
        self.table_name = {}
        self.table_name['yahoo_stock_profile'] = 'profile'
        self.table_name['yahoo_stock_summary'] = 'summary'
        self.table_name['yahoo_stock_statistics'] = 'statistics'

 
    def process_item(self, item, spider):

        if spider.name not in self.table_name:
            return item
        try:
            keys = ','.join(item.keys())
            question_marks = ','.join(list('?'*len(item)))
            values = tuple(item.values())
            query = 'INSERT or REPLACE INTO '+self.table_name[spider.name]+' ('+keys+') VALUES ('+question_marks+')'
            #print query
            cur = self.conn.cursor()
            cur.execute(query, values)
            self.conn.commit()
            #ser_item = pd.Series(item)
            #ser_item.to_sql('profile',self.conn,if_exists='append')
            print('Succeed to insert item: ' + item['Symbol'])
        except:
            try:
                create_query = """CREATE TABLE IF NOT EXISTS %s
                     (%s, Primary Key (Symbol, EventDate))
                     """%(self.table_name[spider.name],','.join(['%s text'%k for k in item.keys()]))
                print(create_query)
                self.conn.execute(create_query)
                self.conn.commit()
                cur = self.conn.cursor()
                cur.execute(query, values)
                self.conn.commit()
            except:
                print('Failed to insert item: ' + item['Symbol'])
        return item


