# -*- coding: utf-8 -*-

import re
import pandas as pd
import datetime as dt
import glob
import os

class LoadStockSymbol(object):
    def __init__(self,file_name=''):
        if file_name:
            self.file_name = file_name
        else:
            abs_path = os.path.dirname(os.path.realpath(__file__))
            self.file_name = os.path.join(abs_path,'tickers/SymbolList.csv')

        #self.file_name = '/Users/donglin/Dropbox/Workspace/invest/invest/tickers/SymbolList.csv'
        
    def load(self):
        out = []
        handle = open(self.file_name)
        header = handle.next().strip().split(',')
        for line in handle:
            data = dict(zip(header,line.strip().split(',')))
            out.append(data['Symbol'])
        return out

class FormatDbFieldName(object):
    def __init__(self):
        self.char_replaced = {}
        self.char_replaced['@'] = 'at'
        self.char_replaced['&'] = 'and'
        self.char_replaced['%'] = 'Pcnt'
        self.char_replaced['-'] = '_'
        self.char_replaced['/'] = '_over_'
        self.char_replaced[' '] = '_'

    def format_name(self,name):
        for e, v in (self.char_replaced).items():
            name = name.replace(e,v)
        name = re.sub('[^A-Za-z0-9_]+', '', name)
        if name[0] in [str(i) for i in range(10)]:
            name = '_' + name
        return name

class MergeData(object):
    def __init__(self):
        abs_path = os.path.dirname(os.path.realpath(__file__))
        self.ref_file = os.path.join(abs_path,'tickers/SymbolList.csv')
        self.profile_file = sorted(glob.glob(os.path.join(abs_path,'csv/yahoo_stocks_*.profile.csv')))[-1]
        self.statistics_files = sorted(glob.glob(os.path.join(abs_path,'csv/yahoo_stocks_*.statistics.csv')))
        self.summary_files = sorted(glob.glob(os.path.join(abs_path,'csv/yahoo_stocks_*.summary.csv')))
        

    def gen_snapshot(self):
        df = pd.read_csv(self.ref_file)
        profile = pd.read_csv(self.profile_file)
        profile.drop(['Timestamp','EventDate'],axis=1,inplace=True)
        df = pd.merge(df,profile,on='Symbol',how='left')
        summary = pd.read_csv(self.summary_files[-1])
        summary_date = re.search('\d{8}',self.summary_files[-1]).group(0)
        summary.drop(['Timestamp','EventDate','Bid','Ask'],axis=1,inplace=True)
        df1= pd.merge(df,summary,on='Symbol',how='left')
        #print df1.head()
        df1.to_csv('yahoo_summary_snapshot_%s.csv'%summary_date,index=False)
        statistics = pd.read_csv(self.statistics_files[-1])
        statistics_date = re.search('\d{8}',self.statistics_files[-1]).group(0)
        statistics.drop(['Timestamp','EventDate'],axis=1,inplace=True)
        df1= pd.merge(df,statistics,on='Symbol',how='left')
        #print df1.head()
        df1.to_csv('yahoo_statistics_snapshot_%s.csv'%statistics_date,index=False)

    def gen_trending(self):
        df = pd.read_csv(self.ref_file)
        profile = pd.read_csv(self.profile_file)
        profile.drop(['Timestamp','EventDate'],axis=1,inplace=True)
        df = pd.merge(df,profile,on='Symbol',how='left')
        summary_cols = ['Open','Previous_Close','Volume','Market_Cap','PE_Ratio_TTM','Beta','_1y_Target_Est']
        df1 =  {}
        for file_ in self.summary_files:
            summary_date_str = re.search('\d{8}',file_).group(0)
            summary_date = dt.datetime(int(summary_date_str[:4]),int(summary_date_str[4:6]),int(summary_date_str[6:8]))
            summary = pd.read_csv(file_)
            
            for col_ in summary_cols:
                if file_ == self.summary_files[0]:
                    df1[col_] = pd.merge(df,summary[['Symbol',col_]],on='Symbol',how='left')
                else:
                    df1[col_] = pd.merge(df1[col_],summary[['Symbol',col_]],on='Symbol',how='left')
                df1[col_].rename(columns={col_:summary_date.strftime('%a %b-%d-%y')},inplace=True)
                
        writer = pd.ExcelWriter('yahoo_summary_trending_%s.xlsx'%summary_date_str)
        for col_ in summary_cols:
            df1[col_].to_excel(writer,col_,index=False)
        writer.save()



if __name__ == "__main__":
    a = MergeData()
    a.gen_snapshot()
    a.gen_trending()

#print load_stock_symbol()
