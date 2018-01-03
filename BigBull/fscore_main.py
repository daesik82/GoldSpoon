# DB에서 주식코드와 주식명을 불러오고 해당 주식의 재무정보를 가져온다. 그리고 재무정보를 바탕으로 fscore를 계산한다.
# "개별주식 페이지 > 종목분석 >  재무분석"에서 관련 정보 크롤

# Import packages
from datetime import datetime
import time
import random

import numpy as np
import pandas as pd

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# sqlalchemy packages
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, Numeric, String, DateTime, ForeignKey
from sqlalchemy.orm import sessionmaker, relationship, backref
from sqlalchemy.ext.declarative import declarative_base

# Headless Chrome
options = webdriver.ChromeOptions()
#options.add_argument('headless')
#options.add_argument('window-size=1920x1080')
options.add_argument('user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit 537.36 (KHTML, like Gecko) Chrome')

# Start Chrome with Selenium
driver = webdriver.Chrome('/Users/daesikkim/Downloads/chromedriver', chrome_options=options) # chrome_options=options
driver.implicitly_wait(3)

# Get the stock data from DB(postgresql)
# 아래의 sqlalchemy setting과 mapping은 독립해야함
db_string = "postgresql://daesik:@localhost/db_piotroski"

engine = create_engine(db_string, echo=True)

Session = sessionmaker(bind=engine)
session = Session()

Base = declarative_base()


# Mapping
class Stock(Base):
    __tablename__ = 'stocks'

    stock_id = Column(Integer, primary_key=True)
    stock_code = Column(String, unique=True, nullable=False, primary_key=True)
    company = Column(String, index=True, unique=True, nullable=False)
    market_type = Column(Integer)
    m_ics = Column(String)
    w_ics = Column(String)
    created_on = Column(DateTime, default=datetime.utcnow)
    updated_on = Column(DateTime, default=datetime.utcnow)

    base_param = relationship('BaseParam', back_populates='stock')
    bookmarket_param = relationship('BookMarketParam', back_populates='stock')

    # def __repr__(self):
    #    return "<Stock ==> id : {0}, code : {1}, company : {2}, market_type : {3}, created : {4}, updated : {5}>".format(self.stock_id,
    #                                                                                                                     self.stock_code,
    #                                                                                                                     self.company,
    #                                                                                                                     self.market_type,
    #                                                                                                                     self.created_on,
    #                                                                                                                     self.updated_on,)


class BaseParam(Base):
    __tablename__ = 'base_params'

    baseparam_id = Column(Integer, primary_key=True)
    stock_code = Column(String, ForeignKey('stocks.stock_code'))
    date = Column(DateTime)
    price_open = Column(Integer)
    price_close = Column(Integer)
    price_high = Column(Integer)
    price_low = Column(Integer)
    quant = Column(Integer)
    market_sum = Column(Integer)

    stock = relationship("Stock", back_populates="base_param")


class BookMarketParam(Base):
    __tablename__ = 'bookmarket_params'

    bookmarketparam_id = Column(Integer, primary_key=True)
    stock_code = Column(String, ForeignKey('stocks.stock_code'))
    listed_stock_cnt = Column(Integer)
    property_total = Column(Integer)
    debt_total = Column(Integer)
    pbr = Column(Integer)

    stock = relationship('Stock', back_populates='bookmarket_param')

Base.metadata.create_all(engine)

# Getting stock codes and names
code_n_name = session.query(Stock.stock_code, Stock.company).all()

code_n_name = dict(code_n_name[0:5]) # Adjust number of codes in the future. For development purpose, codes are limited to 5


# A function to crawl the industry classification, Market & ICS
def crawl_ics(code):
    # pArea > div.wrapper-table > div > table > tbody > tr:nth-child(1) > td > dl > dt:nth-child(3)
    # getting the page source and making the soup!
    html_ics = driver.page_source
    soup_ics = BeautifulSoup(html_ics, 'lxml')

    # getting valus for ICS
    m_ics = soup_ics.select(
        '#pArea > div.wrapper-table > div > table > tbody > tr:nth-of-type(1) > td > dl > dt:nth-of-type(3)')
    w_ics = soup_ics.select(
        '#pArea > div.wrapper-table > div > table > tbody > tr:nth-of-type(1) > td > dl > dt:nth-of-type(4)')

    m_ics = m_ics[0].string.split(' : ')[-1]
    w_ics = w_ics[0].string.split(' : ')[-1]

    ics = {'m_ics': [m_ics] * 5, 'w_ics': [w_ics] * 5}

    return ics


# A function to crawl a balance sheet
def crawl_balsheet(code):
    # click Bal Sheet Tab
    balsheet_tab = driver.find_element_by_css_selector("#rpt_tab2")
    balsheet_tab.click()
    time.sleep(0.8)

    # getting the page source and making the soup!
    html_balsheet = driver.page_source
    soup_balsheet = BeautifulSoup(html_balsheet, 'lxml')

    # getting values from "Balance Sheet"
    total_asset = soup_balsheet.select('#table-content > table > tbody > tr:nth-of-type(1) > td.num')
    lt_debt = soup_balsheet.select('#table-content > table > tbody > tr:nth-of-type(152) > td.num')
    lt_borrowing = soup_balsheet.select('#table-content > table > tbody > tr:nth-of-type(158) > td.num')
    current_asset = soup_balsheet.select('#table-content > table > tbody > tr:nth-of-type(2) > td.num')
    current_liabilities = soup_balsheet.select('#table-content > table > tbody > tr:nth-of-type(110) > td.num')
    shareholder_equity = soup_balsheet.select('#table-content > table > tbody > tr:nth-of-type(188) > td.num')
    stock_issued = soup_balsheet.select('#table-content > table > tbody > tr:nth-of-type(218) > td.num')
    intangible_asset = soup_balsheet.select('#table-content > table > tbody > tr:nth-of-type(62) > td.num')
    book_value1 = soup_balsheet.select('#table-content > table > tbody > tr:nth-of-type(107) > td.num')
    book_value2 = soup_balsheet.select('#table-content > table > tbody > tr:nth-of-type(108) > td.num')
    total_liabilities = soup_balsheet.select('#table-content > table > tbody > tr:nth-of-type(109) > td.num')

    # convert string --> float
    balsheet_vals = list(map(
        lambda l: [float(i.string.replace(',', '')) if i.string != u'\xa0' else float(i.string.replace(u'\xa0', '0'))
                   for i in l[0:5]],
        [total_asset, lt_debt, lt_borrowing, current_asset, current_liabilities, shareholder_equity, stock_issued]))

    balsheet_dict = {'total_asset': balsheet_vals[0],
                     'lt_debt': balsheet_vals[1],
                     'lt_borrowing': balsheet_vals[2],
                     'current_asset': balsheet_vals[3],
                     'current_liabilities': balsheet_vals[4],
                     'shareholder_equity': balsheet_vals[5],
                     'stock_issued': balsheet_vals[6],
                     'intangible_asset': balsheet_vals[7],
                     'book_value1': balsheet_vals[8],
                     'book_value2': balsheet_vals[9],
                     'total_liabilities': balsheet_vals[10]}

    return balsheet_dict


# A function to crawl an income statement
def crawl_incomestate(code):
    # click Income Statement Tab
    incomestate_tab = driver.find_element_by_css_selector("#rpt_tab1")
    incomestate_tab.click()
    time.sleep(0.8)

    # getting the page source and making the soup
    html_incomestate = driver.page_source
    soup_incomestate = BeautifulSoup(html_incomestate, 'lxml')

    # getting values from "Income Statement"
    operating_income = soup_incomestate.select('#table-content > table > tbody > tr:nth-of-type(59) > td.num')
    extra_income = soup_incomestate.select('#table-content > table > tbody > tr:nth-of-type(144) > td.num')
    total_sales = soup_incomestate.select('#table-content > table > tbody > tr:nth-of-type(1) > td.num')
    gross_profit = soup_incomestate.select('#table-content > table > tbody > tr:nth-of-type(26) > td.num')
    cogs = soup_incomestate.select('#table-content > table > tbody > tr:nth-of-type(15) > td.num')

    # convert string --> float
    incomestate_vals = list(map(
        lambda l: [float(i.string.replace(',', '')) if i.string != u'\xa0' else float(i.string.replace(u'\xa0', '0'))
                   for i in l[0:5]], [operating_income, extra_income, total_sales, gross_profit, cogs]))

    incomestate_dict = {'operating_income': incomestate_vals[0],
                        'extra_income': incomestate_vals[1],
                        'total_sales': incomestate_vals[2],
                        'gross_profit': incomestate_vals[3],
                        'cogs': incomestate_vals[4]}

    return incomestate_dict


# A function to crawl a cash flow
def crawl_cashflow(code):
    # click Cash Flow tab
    cashflow_tab = driver.find_element_by_css_selector("#rpt_tab3")
    cashflow_tab.click
    time.sleep(0.8)

    # getting the page source and making the soup
    html_cashflow = driver.page_source
    soup_cashflow = BeautifulSoup(html_cashflow, 'lxml')

    # getting values from "Income Statement"
    cf_operation = soup_cashflow.select('#table-content > table > tbody > tr:nth-of-type(1) > td.num')

    # convert string --> float
    cashflow_vals = list(map(
        lambda l: [float(i.string.replace(',', '')) if i.string != u'\xa0' else float(i.string.replace(u'\xa0', '0'))
                   for i in l[0:5]], [cf_operation]))

    cashflow_dict = {'cf_operation': cashflow_vals[0]}

    return cashflow_dict


# A function to transform a list to DF
def merge_n_convert(code, year, fs_dict):
    dict_individual = {'code': [code] * 5,
                       'year': year}

    dict_individual.update(fs_dict)

    df_individual = pd.DataFrame(dict_individual, columns=('code',
                                                           'm_ics',
                                                           'w_ics',
                                                           'year',
                                                           'total_asset',
                                                           'lt_debt',
                                                           'lt_borrowing',
                                                           'current_asset',
                                                           'current_liabilities',
                                                           'shareholder_equity',
                                                           'stock_issued',
                                                           'intangible_asset',
                                                           'operating_income',
                                                           'extra_income',
                                                           'total_sales',
                                                           'gross_profit',
                                                           'cogs',
                                                           'cf_operation'))

    # Creating f score DF
    year = ['2012', '2013', '2014', '2015', '2016']
    fs_dict = {}
    df_fbase = pd.DataFrame(columns=('code',
                                     'm_ics',
                                     'w_ics',
                                     'year',
                                     'total_asset',
                                     'lt_debt',
                                     'lt_borrowing',
                                     'current_asset',
                                     'current_liabilities',
                                     'shareholder_equity',
                                     'stock_issued',
                                     'intangible_asset',
                                     'operating_income',
                                     'extra_income',
                                     'total_sales',
                                     'gross_profit',
                                     'cogs',
                                     'cf_operation'))
    df_fcalc = pd.DataFrame(columns=('code',
                                     'year',
                                     'cal_roa',
                                     'cal_cfo',
                                     'delta_roa',
                                     'accrual',
                                     'delta_lever',
                                     'delta_liquid',
                                     'eq_offer',
                                     'delta_margin',
                                     'delta_turn'))
    df_fscore = pd.DataFrame(columns=('code',
                                      'year',
                                      'f_roa',
                                      'f_cfo',
                                      'f_droa',
                                      'f_accrual',
                                      'f_dlever',
                                      'f_dliquid',
                                      'f_equityoffer',
                                      'f_dmargin',
                                      'f_dturnover',
                                      'f_total'))

    return df_individual


# One Big Loop through all company codes
for i in code_n_name.keys():

    # load the page!
    driver.get("http://finance.naver.com/item/coinfo.nhn?code={}".format(i))
    time.sleep(random.randrange(2, 8, 1))

    # move to the relavent frame
    frame = WebDriverWait(driver, 10).until(EC.frame_to_be_available_and_switch_to_it((By.CSS_SELECTOR, "#coinfo_cp")))
    # driver.switch_to.frame(frame)

    # select to the financial statement tab
    finstate_tab = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, "#header-menu > div.wrapper-menu > dl > dt:nth-of-type(3) > a")))
    finstate_tab.click()

    code = i
    for d in [crawl_ics(i), crawl_balsheet(i), crawl_incomestate(i), crawl_cashflow(i)]:
        fs_dict.update(d)

    df_company = merge_n_convert(i, year, fs_dict)

    df_fbase = pd.concat([df_fbase, df_company], ignore_index=True)

    print(df_fbase)

# Cleaning up DataFrame for calculation
# Lagged total asset
df_fbase.l_total_asset = df_fbase.total_asset.shift(1)
df_fbase.l_total_asset = df_fbase.l_total_asset.where(df_fbase.year != "2012")

# Average total asset - "rolling"
df_fbase.avg_total_asset = df_fbase.total_asset.where(df_fbase.year != "2012").rolling(2).mean()

# Calculating relavent values for df_fcalc
df_fcalc.code = df_fbase.code
df_fcalc.year = df_fbase.year

df_fcalc.cal_roa = df_fbase.operating_income / df_fbase.l_total_asset
df_fcalc.cal_cfo = df_fbase.cf_operation / df_fbase.l_total_asset
df_fcalc.delta_roa = df_fcalc.cal_roa.diff()
df_fcalc.accrual = df_fcalc.cal_roa - df_fcalc.cal_cfo

df_fcalc.lever = df_fbase.lt_debt / df_fbase.avg_total_asset
df_fcalc.delta_lever = df_fcalc.lever.diff()
df_fcalc.delta_liquid = df_fbase.current_asset.where(df_fbase.year != '2012') / df_fbase.current_liabilities.where(df_fbase.year != '2012')
df_fcalc.eq_offer = df_fbase.stock_issued.where(df_fbase.year != '2012').diff()

df_fcalc.gmo = df_fbase.gross_profit.where(df_fbase.year != '2012') / df_fbase.total_sales.where(df_fbase.year != '2012')
df_fcalc.delta_margin = df_fcalc.gmo.diff()
df_fcalc.atr = df_fbase.total_sales / df_fbase.l_total_asset
df_fcalc.delta_turn = df_fcalc.atr.diff()

# code & year
df_fscore.code = df_fbase.code
df_fscore.year = df_fbase.year

# ROA and its f-score in DataFrame
df_fscore.f_roa = np.where(df_fcalc.cal_roa > 0, 1, 0)

# CFO and its f-score in DataFrame
df_fscore.f_cfo = np.where(df_fcalc.cal_cfo > 0, 1, 0)

# delta ROA and its f-score in DataFrame
df_fscore.f_droa = np.where(df_fcalc.delta_roa > 0, 1, 0)

# accrual and its f-score
df_fscore.f_accrual = np.where(df_fcalc.accrual < 0, 1, 0)

# delta_leverage and its f-score
df_fscore.f_dlever = np.where(df_fcalc.delta_lever < 0, 1, 0)

# delta_liquidity and its f-score
df_fscore.f_dliquid = np.where(df_fcalc.delta_liquid > 0, 1, 0)

# equity offer and its f-score
df_fscore.f_equityoffer = np.where(df_fcalc.eq_offer > 0, 0, 1)

# delta_margin and its f-score
df_fscore.f_dmargin = np.where(df_fcalc.delta_margin > 0, 1, 0)

# delta_turnover and its f-score
df_fscore.f_dturnover = np.where(df_fcalc.delta_turn > 0, 1, 0)

# total f-score
df_fscore.f_total = df_fscore.f_roa \
                    + df_fscore.f_cfo \
                    + df_fscore.f_droa \
                    + df_fscore.f_accrual \
                    + df_fscore.f_dlever \
                    + df_fscore.f_dliquid \
                    + df_fscore.f_equityoffer \
                    + df_fscore.f_dmargin \
                    + df_fscore.f_dturnover

