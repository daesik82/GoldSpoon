"""

"""
import time
import random

import requests
from bs4 import BeautifulSoup

from .base_crawler import BaseCrawler
from ..data_structure.stock_code_bucket import StockCodeBucket
from BigBull import KOSPI, KOSDAK

__all__ = ['CompanyInformationCrawler']

bucket_of_urls = {
    'com_income': 'http://companyinfo.stock.naver.com/v1/company/cF3002.aspx?cmp_cd={}&frq=0&rpt=0&finGubun=MAIN\
                   &frqTyp=0&cn=',
    'fin_position': 'http://companyinfo.stock.naver.com/v1/company/cF3002.aspx?cmp_cd={}&frq=0&rpt=1&finGubun=MAIN\
                   &frqTyp=0&cn=',
    'cash_flow': 'http://companyinfo.stock.naver.com/v1/company/cF3002.aspx?cmp_cd={}&frq=0&rpt=2&finGubun=MAIN\
                  &frqTyp=0&cn='
                }

want_to_get_dict = {
    'com_income': [
                    '매출액(수익)',
                    '*내수',
                    '*수출',
                    '매출원가',
                    '매출총이익',
                    '영업이익',
                    '기타영업외손익',
                    '계속사업이익',
                    '당기순이익',
                    '*EBITDA',
                    '*EBIT'],

    'fin_position': [
                    '자산총계',
                    '유동자산',
                    '....재고자산',
                    '비유동자산',
                    '....유형자산',
                    '....무형자산',
                    '*순자산1',
                    '*순자산2',
                    '부채총계',
                    '유동부채',
                    '....단기차입금',
                    '비유동부채',
                    '....장기차입금',
                    '*CAPEX',
                    '자본총계',
                    '지배주주지분',
                    '발행주식수'],

    'cash_flow': ['영업활동으로인한현금흐름']

                    }


def listdata_to_dict(ls):
    return {
        'code': ls[0],
        'year': ls[1],
        'total_sales': ls[2],
        'domestic_sales': ls[3],
        'export_sales': ls[4],
        'cogs': ls[5],
        'gross_profit': ls[6],
        'operating_income': ls[7],
        'extraordinary_items_income': ls[8],
        'cont_operating_income': ls[9],
        'net_income': ls[10],
        'ebitda': ls[11],
        'ebit': ls[12],
        'total_assets': ls[13],
        'current_assets': ls[14],
        'inventories': ls[15],
        'non_current_assets': ls[16],
        'tangible_assets': ls[17],
        'intangible_assets': ls[18],
        'book_value_1': ls[19],
        'book_value_2': ls[20],
        'total_liabilities': ls[21],
        'current_liabilities': ls[22],
        'short_term_borrowings': ls[23],
        'non_current_liabilities': ls[24],
        'long_term_borrowings': ls[25],
        'capital_expenditure': ls[26],
        'total_equity': ls[27],
        'equity_owers': ls[28],
        'stock_issued': ls[29],
        'cashflow_operations': ls[30]

            }


# 원하는 연도의 데이터를 가져오기 위한 함수
def clean_get_year(years, data):
    year_list = [v.split('/')[0] for i, v in enumerate(data[:-2]) if '(E)' not in v]
    index_list = [i for i, v in enumerate(data[:-2]) if '(E)' not in v]
    year_list_copy = year_list.copy()
    del_count = 0
    if years is not None:
        for index, year in enumerate(year_list_copy):
            if year not in years:
                del year_list[index - del_count]
                del index_list[index - del_count]
                del_count += 1

    return year_list, index_list


def clean_decimal_point(num):
    if num is None:
        return None
    else:
        return round(float(num),2)


class CompanyInformationCrawler(BaseCrawler):
    """

    """

    def __init__(self, db):
        super().__init__(db)

    def get_data_from_separate_url(self, code, years, url_type):
        url = bucket_of_urls[url_type].format(code)
        req = requests.get(url).json()
        data_list = []

        if req['DATA'] is not None:  # 특정 종목에 대해서 재무제표가 존재하지 않음 ex. 005935 삼성전자우
            year, index = clean_get_year(years, req['YYMM'])
            data_list.append([code] * len(year))
            data_list.append(year)
            print('총 {}개의 연도에 해당하는 {} 정보를 가져왔습니다.'.format(len(index), url_type))

            for row in req['DATA']:
                if row['ACC_NM'] in want_to_get_dict[url_type]:
                    data_list.append([clean_decimal_point(row['DATA{}'.format(i + 1)]) for i in index])
            return data_list

        else:
            return None

    def crawl_company_information(self, code, years=None):

        com_income = self.get_data_from_separate_url(code, years, 'com_income')
        fin_position = self.get_data_from_separate_url(code, years, 'fin_position')
        cash_flow = self.get_data_from_separate_url(code, years, 'cash_flow')
        fail = False
        dict_of_information = None

        if com_income is None and fin_position is None and cash_flow is None:
            print('----------재무정보가 존재하지 않는 종목번호입니다.--------------')

        else:
            information_zip = list(zip(*com_income, *fin_position[2:], *cash_flow[2:]))

            try:
                dict_of_information = [listdata_to_dict(info) for info in information_zip]
                print(f'{code}의 재무정보를 가져왔습니다.')
            except IndexError:
                fail = True
                print(f"{code}는 은행, 보험업의 재무정보이기 때문에 가져올 수 없습니다.")

        return fail, dict_of_information

    def save_company_information(self, years=None):
        count = 1
        is_okay = True

        len_company = len([code for code, year in self.db.load_all_stock_code()])
        company_information_data_list = []
        fail_list = []

        for name, code in self.db.load_all_stock_code():

            fail, result = self.crawl_company_information(code, years)
            if result is not None: # 정상적으로 회사 재무정보를 가져오는 경우.
                company_information_data_list += result
                time.sleep(random.uniform(0.1, 1))

            else:
                if fail is True: # 회사의 재무정보가 페이지에 존재하지만, 은행업과 보험업이라서 파싱 x / 해당 종목의 경우 fali_list에 저장.
                    fail_list.append((name, code))
                    time.sleep(random.uniform(0.1, 1))
            print(f'{count}/{len_company}이 완료되었습니다.')
            count += 1

        is_okay *= self.db.insert_multiple('company_info',
                                            company_information_data_list)

        if is_okay:
            print(f"{len_company}개의 크롤이 완료되고 저장되었습니다.")

        return fail_list, company_information_data_list

