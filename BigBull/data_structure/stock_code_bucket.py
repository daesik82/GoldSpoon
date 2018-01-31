"""

"""

__all__ = ['StockCodeBucket']

class BaseCollection:
    """
    StockCode 클래스에서 사용

    """
    def __init__(self):
        self.items = {}

    def clear(self):
        self.items.clear()

    def count(self):
        return len(self.items)

    def countItem(self, column):
        # item 의 value 가 list 인듯
        return len(self.items[column])

    def find(self, column):
        # None 리턴하는거 나중에 코드 이해하고 최대한 없애도록 해야겠다
        return self.items.get(column, None)

    def iterItems(self):
        """
        Generator 식으로 dict 에 있는 key, value 반환
        :return:
        """
        return self.items.items()


class StockCodeItem:
    """
    종목의 기본정보를 담고있는 클래스
    """
    def __init__(self, market_type, code, company):
        self.market_type = market_type
        self.code = code
        self.company = company


class StockCodeBucket(BaseCollection):
    """
    종목코드를 관리하기 위한 것으로 코드의 추가 삭제 검색과 종목 수 등을 알려준다
    """
    def add(self, market_type, code, company):
        """
        종목코드를 추가하는 함수

        :param market_type: 시장종류 1=코스피 2=코스닥
        :param code: 종목코드
        :param company: 종목명

        """
        a_item = StockCodeItem(market_type, code, company)
        self.items[code] = a_item

    def remove(self, stock_code):
        """
        종목을 삭제하는 함수
        :param stock_code: 종목코드

        """
        del self.items[stock_code]

    def dump(self):
        """
        현재 클래스가 가지고있는 모든 종목에대한 정보를 출력하는 함수다
        """
        for index, stock in enumerate(self.iterItems()):
            # index : 0, 1, 2, 3,
            # stock_code_item : (key1, value1), (key2, value2)...
            code = stock[0]
            stock_code_item = stock[1]
            print(f"{index+1}. market_type:{stock_code_item.market_type} - {stock_code_item.company}({code})")
