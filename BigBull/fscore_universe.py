# F-Score Universe 설정을 위한 데이터 수집 및 정리
# "네이버 증권 > 국내증시 > 시가총액" 에서 시장별 주식명, 주식코드, 주식관련 일부 데이터 크롤

# Importing packages
import requests
from bs4 import BeautifulSoup

import numpy as np
import pandas as pd

# Setting an URL for crawling
# 네이버 시가총액 페이지에서 6가지 옵션을 선택할 수 있고, 옵션코드는 아래와 같다.
"""options to choose 
   1. 거래량 : 'quant'
   2. 시가 : 'open_val'
   3. 고가 : 'high_val'
   4. 저가 : 'loq_val' 
   5. 매수호가 : 'ask_buy'
   6. 매도호가 : 'ask_sell' 
   7. 매수총잔량 : 'buy_total'
   8. 매도총잔량 : 'sell_total'
   9. 거래대금(백만) : 'amount' 
   10. 전일거래량 : 'prev_quant'
   11. 외국인비율 : 'frgn_rate'
   12. 상장주식수(천주) : 'listed_stock_cnt'
   13. 시가총액(억) : 'market_sum'
   14. 자산총계(억) : 'property_total'
   15. 부채총계(억) : 'debt_total' 
   16. 매출액(억) : 'sales'
   17. 매출액증가율 : 'sales_increasing_rate'
   18. 영업이익(억) : 'operating_profit'
   19. 영업이익증가율 : 'operating_profit_increasing_rate' 
   20. 당기순이익(억) : 'net_income'
   21. 주당순이익(원) : 'eps'
   22. 보통주배당금(원) : 'dividend'
   23. PER(배) : 'per'
   24. ROE(%) : 'roe'
   25. ROA(%) : 'roa'
   26. PBR(배) : 'pbr'
   27. 유보율(%) : 'reserve_ratio'
   """

url = "http://finance.naver.com/sise/field_submit.nhn?menu=market_sum&returnUrl=http%3A%2F%2Ffinance.naver.com%2Fsise" \
      "%2Fsise_market_sum.nhn%3Fsosok%3D{0}%26page%3D{1}&fieldIds={2}&fieldIds={3}&fieldIds={4}&fieldIds={5}&fieldIds=" \
      "{6}&fieldIds={7}".format('0',
                                '1',
                                'quant',
                                'listed_stock_cnt',
                                'market_sum',
                                'property_total',
                                'debt_total',
                                'pbr')

# Send Requests and making Soup
req = requests.get(url)
html = req.text
soup = BeautifulSoup(html, 'lxml')

# Getting information from KOSPI pages
page_num = 1  # KOSPI 27 pages

stock_id_ls = []
stock_name_ls = []

all_number_classes = []

for num in range(1, page_num + 28):
    url_p = "http://finance.naver.com/sise/field_submit.nhn?menu=market_sum&returnUrl=http%3A%2F%2Ffinance.naver.com%" \
            "2Fsise%2Fsise_market_sum.nhn%3Fsosok%3D{0}%26page%3D{1}&fieldIds={2}&fieldIds={3}&fieldIds={4}&fieldIds=" \
            "{5}&fieldIds={6}&fieldIds={7}".format('0',
                                                   num,
                                                   'quant',
                                                   'listed_stock_cnt',
                                                   'market_sum',
                                                   'property_total',
                                                   'debt_total',
                                                   'pbr')

    r = requests.get(url_p)
    html = r.text

    soup = BeautifulSoup(html, 'lxml')

    stock_ids = soup.select('a[href*="/item/main.nhn"]')
    piotroski_univ_sets = soup.select('td[class^="number"]')

    # Looping stock ids and names
    for id in stock_ids:
        stock_id_ls.append(str(id).strip('"<>/a').split('=')[-1].split('">')[0])
        stock_name_ls.append(str(id).strip('"<>/a').split('=')[-1].split('">')[1])

    # Looping Number Classes for each stock
    for item in piotroski_univ_sets:
        all_number_classes.append(item.string)

# Convert stock ids & names lists to Pandas DF
df_kospi = pd.DataFrame({'id' : stock_id_ls, 'name': stock_name_ls, 'market_type': 1}, columns = ['id',
                                                                                                  'name',
                                                                                                  'market_type',
                                                                                                  'quant',
                                                                                                  'market_sum',
                                                                                                  'property_total',
                                                                                                  'debt_total',
                                                                                                  'listed_stock_cnt',
                                                                                                  'pbr',
                                                                                                  'face_value',])

# Slicing and creating an individual list for each category
present_value = all_number_classes[::10]
face_value = all_number_classes[3::10]
quant = all_number_classes[4::10]
listed_stock_cnt = all_number_classes[5::10]
market_sum = all_number_classes[6::10]
property_total = all_number_classes[7::10]
debt_total = all_number_classes[8::10]
pbr = all_number_classes[9::10]

# Put each category list to DF
df_kospi['present_value'] = present_value
df_kospi['quant'] = quant
df_kospi['property_total'] = property_total
df_kospi['debt_total'] = debt_total
df_kospi['listed_stock_cnt'] = listed_stock_cnt
df_kospi['pbr'] = pbr
df_kospi['market_sum'] = market_sum
df_kospi['face_value'] = face_value

# Getting information from KOSDAQ pages
page_num = 1  # KOSDAQ 26pages

kosdaq_id_ls = []
kosdaq_name_ls = []

kosdaq_number_classes = []

for num in range(1, page_num + 27):
    url_p = "http://finance.naver.com/sise/field_submit.nhn?menu=market_sum&returnUrl=http%3A%2F%2Ffinance.naver.com%" \
            "2Fsise%2Fsise_market_sum.nhn%3Fsosok%3D{0}%26page%3D{1}&fieldIds={2}&fieldIds={3}&fieldIds={4}&fieldIds=" \
            "{5}&fieldIds={6}&fieldIds={7}".format('1',
                                                   num,
                                                   'quant',
                                                   'listed_stock_cnt',
                                                   'market_sum',
                                                   'property_total',
                                                   'debt_total',
                                                   'pbr')

    r = requests.get(url_p)
    html = r.text

    soup = BeautifulSoup(html, 'lxml')

    kosdaq_ids = soup.select('a[href*="/item/main.nhn"]')
    piotroski_univ_sets_kosdaq = soup.select('td[class^="number"]')

    # Looping KOSDAQ ids and names
    for id in kosdaq_ids:
        kosdaq_id_ls.append(str(id).strip('"<>/a').split('=')[-1].split('">')[0])
        kosdaq_name_ls.append(str(id).strip('"<>/a').split('=')[-1].split('">')[1])

    # Looping Number classes for each stock
    for item in piotroski_univ_sets_kosdaq:
        kosdaq_number_classes.append(item.string)

# convert KOSDAQ ids & names lists to Pandas DF
df_kosdaq = pd.DataFrame({'id' : kosdaq_id_ls, 'name' : kosdaq_name_ls, 'market_type' : 2}, columns = ['id',
                                                                                                       'name',
                                                                                                       'market_type',
                                                                                                       'quant',
                                                                                                       'market_sum',
                                                                                                       'property_total',
                                                                                                       'debt_total',
                                                                                                       'listed_stock_cnt',
                                                                                                       'pbr', 'face_value',
                                                                                                       ])

# Slicing and Put into df_kosdaq
# Slicing and creating an individual list for each category
present_value = kosdaq_number_classes[::10]
face_value = kosdaq_number_classes[3::10]
quant = kosdaq_number_classes[4::10]
listed_stock_cnt = kosdaq_number_classes[5::10]
market_sum = kosdaq_number_classes[6::10]
property_total = kosdaq_number_classes[7::10]
debt_total = kosdaq_number_classes[8::10]
pbr = kosdaq_number_classes[9::10]

# Put each category list to DF
df_kosdaq['present_value'] = present_value
df_kosdaq['quant'] = quant
df_kosdaq['property_total'] = property_total
df_kosdaq['debt_total'] = debt_total
df_kosdaq['listed_stock_cnt'] = listed_stock_cnt
df_kosdaq['pbr'] = pbr
df_kosdaq['market_sum'] = market_sum
df_kosdaq['face_value'] = face_value

# Append KOSPI & KOSDAQ
frames = [df_kospi, df_kosdaq]

# Combined DF (KOSPI + KOSDAQ)
df_piotroski_all = pd.concat(frames, ignore_index=True)

# Separating stock codes and names
stocks = df_piotroski_all[['id', 'name', 'market_type']]
stocks = stocks.rename(columns = {'id' : 'stock_code', 'name' : 'company'})

