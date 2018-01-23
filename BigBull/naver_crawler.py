# coding: utf-8

import datetime
import requests
from bs4 import BeautifulSoup


def get_code(stock_type, page_num):

    stocks = []

    for num in range(1, page_num+1):
        url = 'http://finance.naver.com/sise/sise_market_sum.nhn?sosok={0}&page={1}'.format(stock_type, num)

        req = requests.get(url)
        html = req.text

        soup = BeautifulSoup(html, 'html.parser')
        stock_codes = soup.select('table.type_2 td a.tltle')



        for code in stock_codes:
            stocks.append((code.text, code['href'].split('=')[-1])) # 리스트에 리스트 묶음 형태로 (종목명, 종목코드) 순.


    return stocks


def get_historical_data(date, code):



    date = datetime.datetime.strptime(date, "%Y-%m-%d")
    num = 0

    while True:
        url = 'http://finance.naver.com/item/sise_day.nhn?code={0}&page={1}'.format(code, num)
        req = requests.get(url)

        html = req.text
        soup = BeautifulSoup(html, 'html.parser')

        rows = soup.select('.type2 tr')

        for row in rows[2:]:
            col = row.find_all('td')

            try:
                stock_date = datetime.datetime.strptime(col[0].text, "%Y.%m.%d")
                print(stock_date, col[1].text, col[3].text, col[4].text, col[5].text, col[6].text)
                """
                col[1] : 종가, col[3] : 시가, col[4] : 고가, col[5] : 저가, col[6] : 거래량
                """
                if date > (stock_date + datetime.timedelta(days=1)):
                    print('원하시는 날짜까지 데이터 추출이 완료되었습니다.')
                    return False

            except ValueError:
                pass







        num += 1

def main():

    date = '2017-06-01'  # 원하는 날짜 선택 (지정 날짜부터 가장 최근 데이터까지 가져옴)
    stock_type = 0 # 0이면 코스피 ,1이면 코스닥
    page_num = 27 # 코스피는 27, 코스닥은 25
    stocks = get_code(stock_type, page_num)

    for name, code in stocks: # stocks list는 ( 종목명, 코드명 ) 으로 이뤄진 이중 리스트.
        print("{}의 과거 데이터를 가지고 오는 중입니다.".format(name))
        get_historical_data(date, code)


if __name__ == "__main__":
    main()
