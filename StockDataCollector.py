#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np 
# linear algebra
import pandas as pd 
# pandas for dataframe based data processing and CSV file I/O

import requests # for http requests

from bs4 import BeautifulSoup # for html parsing and scraping
import bs4

from multiprocessing.dummy import Pool as ThreadPool 

from fastnumbers import isfloat 
from fastnumbers import fast_float

import matplotlib.pyplot as plt
import seaborn as sns
import json
from tidylib import tidy_document # for tidying incorrect html

from datetime import date #Record data collection date at the first column

from csv import DictWriter


# In[3]:


def ffloat(string):
    if string is None:
        return np.nan
    if type(string)==float or type(string)==np.float64:
        return string
    if type(string)==int or type(string)==np.int64:
        return string
    return fast_float(string.split(" ")[0].replace(',','').replace('%',''),
                      default=np.nan)


# In[4]:


def get_scrip_info(url):
    original_url = url
    key_val_pairs = {}
    
    page_response = requests.get(url, timeout=240)
    page_content = BeautifulSoup(page_response.content, "html.parser")
    
    today = date.today()
    
    data_date = today.strftime("%B %d, %Y")
    stock_name = page_content.find('h1', attrs = {'class':'D(ib) Fz(18px)','data-reactid':"7"}).text 
    previous_close = ffloat(page_content.find('td',attrs={'data-test':'PREV_CLOSE-value'}).text) 
    open_          = ffloat(page_content.find('td',attrs={'data-test':'OPEN-value'}).text)
    bid            = ffloat(page_content.find('td',attrs={'data-test':'BID-value'}).text)
    ask            = ffloat(page_content.find('td',attrs={'data-test':'ASK-value'}).text)
    day_range      = page_content.find('td',attrs={'data-test':'DAYS_RANGE-value'}).text
    _52week_range  = page_content.find('td',attrs={'data-test':'FIFTY_TWO_WK_RANGE-value'}).text
    volume         = ffloat(page_content.find('td',attrs={'data-test':'TD_VOLUME-value'}).text)
    avg_volume     = ffloat(page_content.find('td',attrs={'data-test':'AVERAGE_VOLUME_3MONTH-value'}).text)
    market_cap     = page_content.find('td',attrs={'data-test':'MARKET_CAP-value'}).text
    beta_5Y_Monthly= ffloat(page_content.find('td',attrs={'data-test':'BETA_5Y-value'}).text)
    PE_TTM         = ffloat(page_content.find('td',attrs={'data-test':'PE_RATIO-value'}).text)
    EPS_TTM        = ffloat(page_content.find('td',attrs={'data-test':'EPS_RATIO-value'}).text)
    Earning_date= page_content.find('td',attrs={'data-test':'EARNINGS_DATE-value'}).text
    forward_div_yeild= page_content.find('td',attrs={'data-test':'DIVIDEND_AND_YIELD-value'}).text
    ex_div_date    = page_content.find('td',attrs={'data-test':'EX_DIVIDEND_DATE-value'}).text
    _1y_target_est = ffloat(page_content.find('td',attrs={'data-test':'ONE_YEAR_TARGET_PRICE-value'}).text) 
    
    #data_table = list()
    #collector = {row[0]:ffloat(row[1]) if len(row)==2 else None for row in data_table}
    #volume = ffloat(page_content.find('span',attrs={'id':'nse_volume'}).text)
    key_val_pairs["data_date"] = data_date
    key_val_pairs["stock_name"] = stock_name
    key_val_pairs["previous_close"] = ffloat(previous_close)
    key_val_pairs["open"] = ffloat(open_)
    key_val_pairs["bid"] = bid
    key_val_pairs["ask"] = ask
    key_val_pairs["day_range"] = day_range
    key_val_pairs["52week_range"] = _52week_range
    key_val_pairs['volume'] = volume
    key_val_pairs['avg_volume'] = avg_volume
    
    key_val_pairs["market_cap"] = market_cap
    key_val_pairs["beta_5Y_Monthly"] = beta_5Y_Monthly
    key_val_pairs["PE_TTM"] = PE_TTM
    key_val_pairs["EPS_TTM"] = EPS_TTM
    key_val_pairs["Earning_date"] = Earning_date
    key_val_pairs['forward_div_yeild'] = forward_div_yeild
    key_val_pairs['ex_div_date'] = ex_div_date
    key_val_pairs['_1y_target_est'] = _1y_target_est
    
    
    return key_val_pairs


# In[5]:


stocks_list = np.array([
    "https://finance.yahoo.com/quote/DE?p=DE", 
    "https://finance.yahoo.com/quote/ETN?p=ETN&.tsrc=fin-srch",    
    "https://finance.yahoo.com/quote/ZM?p=ZM&.tsrc=fin-srch",
    "https://finance.yahoo.com/quote/ISRG?p=ISRG&.tsrc=fin-srch",
    "https://finance.yahoo.com/quote/PYPL?p=PYPL&.tsrc=fin-srch",
    "https://finance.yahoo.com/quote/DHI?p=DHI&.tsrc=fin-srch",
    "https://finance.yahoo.com/quote/UNH?p=UNH&.tsrc=fin-srch",
    "https://finance.yahoo.com/quote/SQ?p=SQ&.tsrc=fin-srch",
    "https://finance.yahoo.com/quote/TSM?p=TSM&.tsrc=fin-srch",
    "https://finance.yahoo.com/quote/JNJ?p=JNJ&.tsrc=fin-srch",
    "https://finance.yahoo.com/quote/MMM?p=MMM&.tsrc=fin-srch",
    "https://finance.yahoo.com/quote/TSLA?p=TSLA&.tsrc=fin-srch",
    "https://finance.yahoo.com/quote/AMD?p=AMD&.tsrc=fin-srch",
    "https://finance.yahoo.com/quote/AAPL?p=AAPL&.tsrc=fin-srch",
    "https://finance.yahoo.com/quote/MSFT?p=MSFT&.tsrc=fin-srch",
    "https://finance.yahoo.com/quote/LMT?p=LMT&.tsrc=fin-srch",
    "https://finance.yahoo.com/quote/FDX?p=FDX&.tsrc=fin-srch",
    "https://finance.yahoo.com/quote/LIN?p=LIN&.tsrc=fin-srch",
    "https://finance.yahoo.com/quote/DIS?p=DIS&.tsrc=fin-srch",
    "https://finance.yahoo.com/quote/APD?p=APD&.tsrc=fin-srch",
    "https://finance.yahoo.com/quote/VMC?p=VMC&.tsrc=fin-srch",
    "https://finance.yahoo.com/quote/CAT?p=CAT&.tsrc=fin-srch",
    "https://finance.yahoo.com/quote/PG?p=PG&.tsrc=fin-srch"
])


# In[6]:


stock_names = [
    "DE",
    'ETN',
    'ZM',
    'ISRG',
    'PYPL',
    'DHI',
    'UNH',
    'SQ',
    'TSM',
    'JNJ',
    'MMM',
    'TSLA',
    'AMD',
    'AAPL',
    'MSFT',
    'LMT',
    'FDX',
    'LIN',
    'DIS',
    'APD',
    'VMC',
    'CAT',
    'PG'
]


# In[7]:


# list of column names
field_names = ['data_date','stock_name', 'previous_close', 'open', 'bid', 'ask', 'day_range', 
             '52week_range', 'volume', 'avg_volume', 'market_cap', 'beta_5Y_Monthly', 'PE_TTM', 'EPS_TTM', 
             'Earning_date', 'forward_div_yeild', 'ex_div_date', '_1y_target_est']


# In[8]:


#Create .csv files in a designated file location;
#Set up headers/column names.
## Run this function only when initiating new files. 
def data_collector_setup(stocks_list,stock_names):

    for i in range(0,len(stocks_list)):
        url = stocks_list[i]
        new_data = get_scrip_info(url)

        file_type = '.csv'
        stock_file_name = stock_names[i] + file_type
        stock_file_path = 'D:/Mia/Stocks/stock data/'+ stock_file_name

        with open(stock_file_path,'a')  as f_object:
            dictwriter_object = DictWriter(f_object, fieldnames = field_names)
            dictwriter_object.writerow(new_data)
            f_object.close()

        stock_file = pd.read_csv(stock_file_path)
        stock_file.to_csv(stock_file_path, header = field_names, index = False)    


# In[9]:


def data_collector_update(stocks_list,stock_names):
    for i in range(0,len(stocks_list)):
        url = stocks_list[i]
        new_data = get_scrip_info(url)

        file_type = '.csv'
        stock_file_name = stock_names[i] + file_type
        stock_file_path = 'D:/Mia/Stocks/stock data/'+ stock_file_name

        with open(stock_file_path,'a')  as f_object:
            dictwriter_object = DictWriter(f_object, fieldnames = field_names)
            dictwriter_object.writerow(new_data)
            f_object.close()


# In[10]:


#data_collector_setup(stocks_list,stock_names) #Run it at the first time only.


# In[11]:


#Make sure to close all your files before running it for daily data collection. 
data_collector_update(stocks_list,stock_names) 


# In[ ]:




