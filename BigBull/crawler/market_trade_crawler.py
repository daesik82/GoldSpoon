"""
This crawler is to crawl information of market capital(시가총액), pbr, per, quant(거래량),
buy_total(매수총잔량), sell_total(매도총잔량 from 'naver.com.'
"""
# Import required packages
import datetime

import requests
from bs4 import BeautifulSoup
import pandas as pd

from .base_crawler import BaseCrawler


__all__ = ["MarketTradeCrawler"]


def clean_comma(value):
    if ',' in value:
        value = value.replace(',', '')

    return value

class MarketTradeCrawler(BaseCrawler):
    """

    """
    def __init__(self, db):
        super().__init__(db)

    def crawl_market_trade_info(self, market):
        t = datetime.datetime.today()
        crawl_date = t.strftime("%Y-%m-%d")

        if market == 0:
            page_num = 29
            kospi_id_ls = []
            kospi_number_classes = []

            for page in range(1, page_num+1):
                # 네이버 시가총액 페이지 주소
                url = "http://finance.naver.com/sise/field_submit.nhn?menu=market_sum&returnUrl=http%3A%2F%2Ffinance.naver.com%2Fsise%2Fsise_market_sum.nhn%3Fsosok%3D{0}%26page%3D{1}&fieldIds={2}&fieldIds={3}&fieldIds={4}&fieldIds={5}&fieldIds={6}&fieldIds={7}".format(market, page, 'quant', 'buy_total', 'sell_total', 'market_sum', 'per', 'pbr')

                soup = BaseCrawler.make_soup(self, url, parser='lxml')

                kospi_ids = soup.select('a[href*="/item/main.nhn"]')
                kospi_trade_info = soup.select('td[class^="number"]')

                # Looping stock ids and names
                for id in kospi_ids:
                    kospi_id_ls.append(str(id).strip('"<>/a').split('=')[-1].split('">')[0])

                # Looping number classes for each stock
                for item in kospi_trade_info:
                    kospi_number_classes.append(item.string)

            # Convert stock ids & names lists to Pandas DF
            df_kospi = pd.DataFrame({'code': kospi_id_ls, 'market_type': 0}, columns = ['code', 'market_type', 'date', 'present_value', 'quant', 'buy_total', 'sell_total', 'market_sum', 'per', 'pbr'])

            # Slicing and creating an individual list for each category
            present_value = [clean_comma(v) for v in kospi_number_classes[::10]]
            quant = [clean_comma(v) for v in kospi_number_classes[4::10]]
            buy_total = [clean_comma(v) for v in kospi_number_classes[5::10]]
            sell_total = [clean_comma(v) for v in kospi_number_classes[6::10]]
            market_sum = [clean_comma(v) for v in kospi_number_classes[7::10]]
            per = [clean_comma(v) for v in kospi_number_classes[8::10]]
            pbr = [clean_comma(v) for v in kospi_number_classes[9::10]]

            # Put each category list to DF
            df_kospi['present_value'] = pd.to_numeric(present_value, errors='coerce')
            df_kospi['quant'] = pd.to_numeric(quant, errors='coerce')
            df_kospi['market_sum'] = pd.to_numeric(market_sum, errors='coerce')
            df_kospi['buy_total'] = pd.to_numeric(buy_total, errors='coerce')
            df_kospi['sell_total'] = pd.to_numeric(sell_total, errors='coerce')
            df_kospi['pbr'] = pd.to_numeric(pbr, errors='coerce')
            df_kospi['per'] = pd.to_numeric(per, errors='coerce')

            df_kospi['date'] = crawl_date

            return df_kospi

        elif market == 1:
            page_num = 26
            kosdaq_id_ls = []
            kosdaq_number_classes = []

            for page in range(1, page_num+1):
                # 네이버 시가총액 페이지 주소(코스닥)
                url = "http://finance.naver.com/sise/field_submit.nhn?menu=market_sum&returnUrl=http%3A%2F%2Ffinance.naver.com%2Fsise%2Fsise_market_sum.nhn%3Fsosok%3D{0}%26page%3D{1}&fieldIds={2}&fieldIds={3}&fieldIds={4}&fieldIds={5}&fieldIds={6}&fieldIds={7}".format(market, page, 'quant', 'buy_total', 'sell_total', 'market_sum', 'per', 'pbr')

                soup = BaseCrawler.make_soup(self, url, parser='lxml')

                kosdaq_ids = soup.select('a[href*="/item/main.nhn"]')
                kosdaq_univ_sets = soup.select('td[class^="number"]')

                # Looping KOSDAQ ids and names
                for id in kosdaq_ids:
                    kosdaq_id_ls.append(str(id).strip('"<>/a').split('=')[-1].split('">')[0])

                # Looping Number classes for each stock
                for item in kosdaq_univ_sets:
                    kosdaq_number_classes.append(item.string)

            # convert KOSDAQ ids & names lists to Pandas DF
            df_kosdaq = pd.DataFrame({'code': kosdaq_id_ls, 'market_type': 1},
                                     columns=['code', 'market_type', 'date', 'present_value', 'quant',
                                              'buy_total', 'sell_total', 'market_sum', 'per', 'pbr'])

            # Slicing and Put into df_kosdaq
            # Slicing and creating an individual list for each category
            present_value = [clean_comma(v) for v in kosdaq_number_classes[::10]]
            quant = [clean_comma(v) for v in kosdaq_number_classes[4::10]]
            buy_total = [clean_comma(v) for v in kosdaq_number_classes[5::10]]
            sell_total = [clean_comma(v) for v in kosdaq_number_classes[6::10]]
            market_sum = [clean_comma(v) for v in kosdaq_number_classes[7::10]]
            per = [clean_comma(v) for v in kosdaq_number_classes[8::10]]
            pbr = [clean_comma(v) for v in kosdaq_number_classes[9::10]]

            # Put each category list to DF
            df_kosdaq['present_value'] = pd.to_numeric(present_value, errors='coerce')
            df_kosdaq['quant'] = pd.to_numeric(quant, errors='coerce')
            df_kosdaq['buy_total'] = pd.to_numeric(buy_total, errors='coerce')
            df_kosdaq['sell_total'] = pd.to_numeric(sell_total, errors='coerce')
            df_kosdaq['per'] = pd.to_numeric(per, errors='coerce')
            df_kosdaq['pbr'] = pd.to_numeric(pbr, errors='coerce')
            df_kosdaq['market_sum'] = pd.to_numeric(market_sum, errors='coerce')

            df_kosdaq['date'] = crawl_date

            return df_kosdaq

    def save_market_trade_info(self):

        df_kospi_trade = self.crawl_market_trade_info(0)
        print(df_kospi_trade)
        df_kosdaq_trade = self.crawl_market_trade_info(1)
        print(df_kosdaq_trade)

        frames = [df_kospi_trade, df_kosdaq_trade]

        df_combined = pd.concat(frames)
        print(df_combined)

        self.db.insert_dataframe('market_trade_info', df_combined)
        print("Saving Successful")