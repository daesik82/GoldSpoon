"""

"""


from .base_crawler import BaseCrawler
from ..data_structure.stock_code_bucket import StockCodeBucket
from BigBull import KOSPI, KOSDAK

__all__ = ['StockCodeCrawler']

class StockCodeCrawler(BaseCrawler):
    """

    """
    def __init__(self, db):
        super().__init__(db)

    def check_update(self, company):
        """
        해당 회사에 update 된 내용(새로 crawl해야할 것)이 있는지 확인
        :param company:
        :return:
        """

    def crawl_stock_code(self, market_type):
        """

        :param market_type:
        :return:
        """

        ### To Do list
        ### 1. if(check_udate) --> 크롤하기
        ### 2. 총페이지 갯수(ex 코스피 29) 자동적으로 얻어올것

        stock_bucket = StockCodeBucket()

        if market_type == KOSPI:
            page_num = 29 # 코스피 29
        elif market_type == KOSDAK:
            page_num = 26 # 코스닥 26

        for num in range(1, page_num+1):
            url = 'http://finance.naver.com/sise/sise_market_sum.nhn?sosok={0}&page={1}'.format(market_type, num)

            soup = self.make_soup(url)
            stock_codes = soup.select('table.type_2 td a.tltle')

            for code in stock_codes:
                company_name = code.text.replace(';', '') # 삼성전자
                stock_code = code['href'].split('=')[-1]    # 005930
                stock_bucket.add(market_type, stock_code, company_name)

        return stock_bucket

    def save_stock_code(self):
        kospi = 0
        kosdak = 0
        is_okay = True


        for market_type in [KOSPI, KOSDAK]:
            stock_code_list = []

            # 코스피 또는 코스닥 종목을 가져온다.
            stock_bucket = self.crawl_stock_code(market_type)

            # 종목 갯수 계산
            if market_type == KOSPI:
                kospi += stock_bucket.count()
                print(f"kospi: {kospi}")
            elif market_type == KOSDAK:
                kosdak += stock_bucket.count()
                print(f"kosdak: {kosdak}")

            # multiple insert 를 위해서 저장할 데이터를 담은 리스트를 만든다.
            for code, stock in stock_bucket.iterItems():
                temp_dict = {
                    'company': stock.company,
                    'code': stock.code,
                    'market_type': stock.market_type
                }

                stock_code_list.append(temp_dict)

            # 종목코드 DB에 저장한다.
            is_okay *= self.db.insert_multiple(self.db.stocks_db_name,
                                            stock_code_list)

        if(is_okay):
            # 아마도 이게 기본 log 가 될듯
            print(f"코스피 {kospi}개의 종목, 코스닥 {kosdak} 종목의 종모코드 정보가 크롤 및 DB에 저장 완료되었습니다.")

        else:
            print("종목코드를 수집하는 과정에서 에러가 발생하였습니다.")

    def leave_footprint_to_db(self):
        """
        DB에 크롤러 log 남기기
        :return:
        """
        pass