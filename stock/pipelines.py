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

    def create_table(self, table_name, keys):
        try:
            query = """CREATE TABLE IF NOT EXISTS %s (%s, Primary Key (Symbol, EventDate))
                """ % (table_name, ', '.join(['%s text' % k for k in keys]))
            self.conn.execute(query)
            self.conn.commit()
            print(query)
            print('Succeed to create table: %s'%table_name)
            return 0
        except Exception as e:
            print('Failed to create table: %s'%table_name)
            print(query)
            print("Error: %s"%type(e).__name__)
            return 1

    def insert_data(self, table_name, values):
        try:
            question_marks = ', '.join(['?'] * len(values))
            query = 'INSERT or REPLACE INTO %s VALUES (%s)' % (table_name, question_marks)
            cur = self.conn.cursor()
            cur.execute(query, values)
            self.conn.commit()
            print('Succeed to insert the item.')
            return 0
        except Exception as e:
            print('Failed to insert the item.')
            print(query)
            print(values)
            print("Error: %s" % type(e).__name__)
            return 1

    def process_item(self, item, spider):
        if spider.name not in self.table_name:
            return item
        keys = []
        values = []
        for k, v in item.items():
            keys.append('"%s"'%k)
            values.append(v)

        table_name = self.table_name[spider.name]

        rst = self.insert_data(table_name, values)
        if rst:
            self.create_table(table_name, keys)
            self.insert_data(table_name, values)

        return item



