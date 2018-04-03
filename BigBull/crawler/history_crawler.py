"""

"""
import datetime
import secrets
import pprint

import requests
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup


from .base_crawler import BaseCrawler
from ..data_structure.stock_code_bucket import StockCodeBucket
from BigBull import KOSPI, KOSDAK

from sqlalchemy import select, desc

__all__ = ['HistoryCrawler']

USER_AGENT = ['Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_2) \
               AppleWebKit/604.4.7 (KHTML, like Gecko) Version/11.0.2 Safari/604.4.7',
              'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) \
               Chrome/64.0.3282.186 Safari/537.36',
              'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_2) AppleWebKit/537.36 (KHTML, like Gecko)\
               Chrome/64.0.3282.186 Safari/537.36',
              'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_2) AppleWebKit/537.36 (KHTML, like Gecko) \
               Chrome/60.0.3112.113 Whale/1.0.39.16 Safari/537.36'
              ]

YEAR_CHANGED = False  # 병렬구조로 바꿀 때 local로 집어 넣어야함.

# 가격정보 관련
def filter_for_history(tag):
    return (
            tag.name == 'td'
            and 'num' in tag.get('class', [])
            and 'cDn' not in tag.get('class', [])
            and 'cUp' not in tag.get('class', [])
            and 'cFt' not in tag.get('class', [])
    )


def is_the_only_string_within_a_tag(s):
    """Return True if this string is the only child of its parent tag."""
    return s and (s == s.parent.string)


def text_to_num(tag):
    return int(tag.text.replace(",", ""))


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


# 시간관련
def check_99_in_year(tags):
    test = [tag.text.split('.')[0] for tag in tags]
    if (test.count('99')):
        return True, test
    else:
        return False, test


def string_to_datetime(tags):
    global YEAR_CHANGED
    if (YEAR_CHANGED):
        return [datetime.datetime.strptime('19' + tag.text, "%Y.%m.%d") for tag in tags]
    else:
        check_ninenine, test = check_99_in_year(tags)
        if (check_ninenine):
            print("--------99년 발견------------")
            YEAR_CHANGED = True
            index = test.index('99')
            return [datetime.datetime.strptime('20' + tag.text, "%Y.%m.%d") for tag in tags[:index]] + \
                   [datetime.datetime.strptime('19' + tag.text, "%Y.%m.%d") for tag in tags[index:]]
        else:
            return [datetime.datetime.strptime('20' + tag.text, "%Y.%m.%d") for tag in tags]


def listdata_to_dict(ls):
    temp_dict = {
        'code': ls[0],  # 종목코드
        'date': ls[1],  # 날짜
        'open': ls[2],  # 시가
        'high': ls[3],  # 고가
        'low': ls[4],  # 저가
        'close': ls[5],  # 종가
        'volume': ls[6],  # 거래량
    }

    return temp_dict


def check_finalpage(np_array, start_date):
    if(len(np_array[np_array <= start_date])):
        return True
    else:
        return False


class HistoryCrawler(BaseCrawler):
    """

    """
    def __init__(self, db):
        super().__init__(db)

    def base_crawl_method_for_history(self, company_name, start_date=datetime.datetime.min, end_date=datetime.datetime.today):
        """
        회사이름과, 원하는 기간(시작, 마지막 날짜 포함)안의 historical data(시가, 종가, 고가, 저가, 거래량, 수정주가 적용)를
        Daum 에서 가져온다.

        - 크롤기간의 default값은 과거~이 함수가 실행되는 timezone에서의 날짜이다.
        - page 1 부터 requests를 날리는 구조이다.
        - 원하는기간의 데이터가 없는 page는 그냥 넘어간다.
        - 마지막페이지이거나, 원하는 기간내의 데이터를 이미 확보했다면 page 끝까지 requests날리지 않고 break 된다.
        """
        global YEAR_CHANGED
        # start_date, end_date 필터링 및 초기값 설정
        if (type(start_date) == str):
            start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        if (type(end_date) == str):
            end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
        else:
            end_date = end_date()

        page = 1
        FOUND_FINALPAGE = False
        code = self.db.find_stock_code(company_name)

        while True:
            with requests.Session() as s:
                u_agent = secrets.choice(USER_AGENT)
                s.headers = {
                    'User-Agent': u_agent
                }

                req = s.get(f'http://finance.daum.net/item/quote_yyyymmdd_sub.daum?page={page}&code={code}&modify=1')
                html = req.text
                soup = BeautifulSoup(html, 'html.parser')
                d = len(soup.find_all('td', 'datetime2')) * 5
                p = len(soup.find_all(filter_for_history, string=is_the_only_string_within_a_tag))

                price = list(chunks(
                    [text_to_num(tag) for tag in soup.find_all(filter_for_history, string=is_the_only_string_within_a_tag)],
                    5))
                date = string_to_datetime(soup.find_all('td', 'datetime2'))

                # cp = date.copy()
                np_price = np.array(price)
                np_date = np.array(date)

                index = (start_date <= np_date) * (np_date <= end_date)
                price = np_price[index].tolist()
                date = np_date[index].tolist()

                if ((not index.sum()) and (d > 0)):
                    # print(f"page num: {page}")
                    # 현재 페이지에 start_date ~ end_date 의 데이터가 없고
                    # and
                    # 현재 페이지에 (하나라도) 데이터가 있을경우
                    # page +=1 하고 이 페이지는 아무런 작없 없이 pass 한다.
                    # 하지만 이전페이지에서 원하는 데이터를 다 얻었다면 FOUND_FINALPAGE가 TRUE이므로
                    # break 된다.
                    if check_finalpage(np_date, start_date):
                        # print("종료")
                        YEAR_CHANGED = False
                        break

                    page += 1
                    pass
                elif (index.sum() and d > 0):
                    page += 1
                    # pprint.pprint([[code, d] + p for d, p in zip(date, price)])
                    yield [[code, d] + p for d, p in zip(date, price)]
                    FOUND_FINALPAGE = check_finalpage(np_date, start_date)
                    # print(FOUND_FINALPAGE)

                    if (d != 150):
                        # + 현재 페이지가 사이트에서 마지막 페이지 일경우
                        # 다음 페이지는 빈 페이지일 것 이므로 크롤 작업을 break한다.
                        # 마지막 페이지에는 항상 데이터가 30일치 이하로 있다. 그래서 d !=150조건을 썼다.
                        # 마지막 페이지가 30일치 데이터가 있으면, 마지막 페이지이지만 이 조건문에서 걸러내지 못한다.
                        # 하지만 그때는 index.sum()=0 이고 d = 0 이므로 가장 아래의 else 조건에서 break 된다.
                        YEAR_CHANGED = False
                        # print(1)
                        break
                    elif (FOUND_FINALPAGE):
                        # + 현재 페이지에서 원하는 데이터를 찾았다
                        # + 현재 페이지에 내가 원하는 날짜보다 그전날짜의 정보가있다 -> 이것이 마지막 페이지이다.
                        # ex.) 2002년 3월 10일 ~ 데이터를 원했는데 100페이지에서 해당 날짜의
                        # 데이터를 얻었다. 그러면 굳이 101페이지에 넘어가지 않고 break한다.
                        # 만약 1월 18일이 해당 페이지에서 가장 오래된 데이터일 경우는(index.sum()>0 and d>0 and check_finalpage=False)
                        # 이 조건문에서 걸러내지 못한다. 그래서 101페이지로 requests가 일어난다. 하지만 그때는 index.sum()=0, d >0 이므로
                        # 가장 첫번째 if 문으로 넘어 가게 되지만, if 문에 걸려서 break 된다.
                        YEAR_CHANGED = False
                        # print(2)
                        break
                else:
                    # 해당 페이지가 빈 페이지인 경우
                    YEAR_CHANGED = False
                    #print(3)
                    break

    def check_intersection(self, t1_start, t1_end, t2_start, t2_end):
        return (t1_start <= t2_start <= t1_end) or (t2_start <= t1_start <= t2_end)

    def toss_databomb_to_db(self, bigdata, update_mode=False):
        """
        """
        data_count = 0
        total_count = 0
        historical_data_list = []
        is_okay = True

        if (update_mode):
            # UPDATE
            for data in bigdata:
                # 이때 data는 2차원 list 다.
                # ex)
                # [['005930', datetime.datetime(2018, 2, 26, 0, 0),
                #  2364000, 2378000,
                #  2354000, 2369000,
                #  163980],
                # ['005930', datetime.datetime(2018, 2, 23, 0, 0),
                #  2338000, 2390000,
                #  2338000, 2361000,
                #  248466]]
                #print(">>>toss 안")
                #print("Data: ", data)
                data_count += len(data)
                #print("Data_count: ", data_count)
                historical_data_list += [listdata_to_dict(oneday_info) for oneday_info in data]

                # 100도 나중에 상수로 바꿔야한다.
                if (data_count >= 100):
                    total_count += data_count
                    data_count = 0
                    #print("historical: ", historical_data_list)
                    is_okay = self.db.update_multiple('hist_data', 'date', historical_data_list)
                    historical_data_list = []

            if (len(historical_data_list) > 0):
                # bigdata에서 마지막으로 100개의 데이터를 던져줬을 경우(이러면 나머지데이터가 없는 경우가된다.)
                # 이 조건을 체크하지않으면 insert_multiple에 빈 dict이 넘어간다.
                is_okay *= self.db.update_multiple('hist_data', 'date', historical_data_list)
                total_count += data_count


        else:
            # INSERT
            for data in bigdata:
                data_count += len(data)
                #print(f"data_count: {data_count}")
                historical_data_list += [listdata_to_dict(oneday_info) for oneday_info in data]

                if (data_count >= 100):
                    total_count += data_count
                    data_count = 0
                    is_okay = self.db.insert_multiple('hist_data', historical_data_list)
                    historical_data_list = []

            if(len(historical_data_list) > 0):
                # bigdata에서 마지막으로 100개의 데이터를 던져줬을 경우(이러면 나머지데이터가 없는 경우가된다.)
                # 이 조건을 체크하지않으면 insert_multiple에 빈 dict이 넘어간다.
                is_okay *= self.db.insert_multiple('hist_data', historical_data_list)
                total_count += data_count

        return is_okay, total_count

    """
    # case1: 현재 DB는 비었고, 전체기간[과거부터~오늘(최신날짜)] 주가 정보를 가져와서 DB에 저장한다.
    #     -> 새 환경에서 첫번째로 수행될 작업
    # case2: 현재 DB는 비었고, 특정기간[start_date, end_date] 주가 정보를 가져와서 DB에 저장한다.
    # case3: 현재 DB에는 데이터가 있는 상황, 추가된 데이터(db속최신날짜 이후 ~ 크롤시점) 를 가져와서 DB에 저장하는 경우
    #     -> case1후 매일 크롤러가 수행할 작업 
    # case4: 현재 DB에는 데이터가 있는 상황, 수정주가로 UPDATE해야되는 상황
    #     -> 배당, 액면분할 등 의 event로 인해 주가가 update되어야 할 경우 가끔식 수행
    
    # save 경우
    # HARD 모드인지 ,DB에 데이터가 있는지 조건확인
    # 1. 해당 기간의 데이터가 DB에 있는지 없는지 상관없이 원하는 기간내의 데이터를 새로 크롤해와서 DB에 저장하는경우(UPDATE, INSERT 둘 다)
    #  case4
    #  - DB에 있는 날짜의 데이터의 경우에는 가격정보를 UPDATE
    #        - DB에 데이터가 있는지 확인, 교집합이 있는지 확인
    #        - 날짜 교집합을 만들어서 start, end date를 정한다.
    #        - 크롤한다.
    #        - UPDATE 한다.
    #  - DB에 없는 날짜의 데이터의 경우에는 가격정보를 INSERT
    #        - DB에 없는 기간 인덱스를 만든다.
    #        - start, end date를 정한다.
    #        - 크롤한다.
    #        - INSERT 한다.
    #  - DB에 없는 기간을 선택하기 위해서 만드는 index가 여러개일 수 있다. base_crwaler_method 여러번 돌려야 할 수 있다.
    
    # 2. 해당 기간의 데이터가 있을 경우 그 기간은 제외하고 DB에 없는 기간의 데이터만 크롤해서 DB에 저장한다.(INSERT만 일어남)
    #    case1, case2, case3
    #  - DB에 없는 기간을 선택하기 위해서 만드는 index가 여러개일 수 있다. base_crwaler_method 여러번 돌려야 할 수 있다.
    #  - 날짜의 교집합을 제외한 날짜 index을 만든다. start, end date 를 정한다.
    #  - 크롤한다.
    #  - INSERT 한다.
    """

    def save_history_of(self, company_name, start_date="1956-3-3",
                        end_date=datetime.datetime.today,
                        hard_mode=True, ignore_past=True):
        """

        :param company_name:
        :param start_date:
        :param end_date:
        :param hard_mode:
        :param ignore_past: unique_request 처리시 과거영역(왼쪽기간)을 무시하기위해서.
                            DB 에있는 데이터보다 과거에있는 데이터의 경우를 무시한다.(requests도 날라가지 않는다.)
                            ex) 기본적으로 이 함수의 request기간은 1956-3-3~today가 된다.
                            이미 크롤러는 한 번 실행된 상태라 DB에 기본적으로 과거부터 근래의 날짜의 데이터까지 들어가있고 하자.
                            그러면 unique_request는 왼쪽(DB 보다 과거), 오른쪽(DB 보다 미래) 양쪽 기간에대해서 나올 것이다.
                            그러면 왼쪽 unique_request은 1956-3-3부터 시작될것이고 끝은 예를들어 1991-1-29로 끝난다고하자.
                            그러면 hard_mode=True라고 하더라도 왼쪽 unique_request: 1956-3-3~1991-1-29의 대한 데이터를 구하려고
                            크롤러가 작동하게 된다. 이렇게 되면 DB에 데이터는 저장하지 않더라도 결국 홈페이지를 다 훓게 되는경우이다.
                            이 경우를 해결하기 위해서 이 파라미터를 도입.
        :return:
        """
        total_count = 0
        update_count = 0
        insert_count = 0
        insert_okay=False
        update_only = False
        only_ignore_past = False


        # start_date, end_date 필터링 및 초기값 설정
        if (type(start_date) == str):
            start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        if (type(end_date) == str):
            end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
        else:
            end_date = datetime.datetime.strptime(end_date().strftime("%Y-%m-%d"), "%Y-%m-%d")

        table = self.db.find_table('hist_data')  # 클래스에서 바꿔야함.
        code_column = table.c.get('code')
        code = self.db.find_stock_code(company_name)

        print(f"{company_name}({code})의 history 작업을 시작합니다.")

        oldest_date_indb_query = select([table.c.get('date')]).where(code_column == code).order_by(table.c.date).limit(1)
        lastest_date_indb_query = select([table.c.get('date')]).where(code_column == code).order_by(desc(table.c.date)).limit(1)

        oldest_rp = self.db.db_execute(oldest_date_indb_query)
        lastest_rp = self.db.db_execute(lastest_date_indb_query)

        # DB에 데이터가 있을 때 hard_mode여 부에 따라서 UPDATE 와 INSERT
        if (oldest_rp.rowcount):
            # lastest_indb = db에 저장된 데이터중 가장 최근 날짜의 datetime을 가져온다.
            # oldest_date_indb = db에 저장된 데이터중 가장 오래된 날짜의 datetime을 가져온다.
            oldest_date_indb = oldest_rp.first()[0]
            lastest_date_indb = lastest_rp.first()[0]

            # 두 기간의 intersection이 있는지 확인한다.
            if (self.check_intersection(oldest_date_indb, lastest_date_indb, start_date, end_date)):
                print("인터섹션이 있습니다.")
                # 두 기간에대해서 time interval list(datetime)를 만든다.
                db_time_interval = set(
                    map(pd.Timestamp.to_pydatetime, pd.date_range(oldest_date_indb, lastest_date_indb).tolist()))
                request_time_interval = set(
                    map(pd.Timestamp.to_pydatetime, pd.date_range(start_date, end_date).tolist()))

                # db 데이터의 interval과 원하는 interval의 교집합을 구한다.
                intersection = db_time_interval & request_time_interval
                intersection_as_list = list(intersection)
                intersection_as_list.sort()
                intersection_start_date = intersection_as_list[0]
                intersection_end_date = intersection_as_list[-1]

                # intersection 구간에서는 hard_mode인 경우는 UPDATE
                # hard_mode가 아닌 경우는 pass
                if (hard_mode):
                    #print("요기요")
                    #print("inter_start: ", intersection_start_date)
                    #print("inter_end:   ", intersection_end_date)
                    data_bomb = self.base_crawl_method_for_history(company_name, intersection_start_date.strftime("%Y-%m-%d")
                                                              , intersection_end_date.strftime("%Y-%m-%d"))

                    update_okay, update_count = self.toss_databomb_to_db(data_bomb, update_mode=True)
                    if (update_okay):
                        print(f"intersection {intersection_start_date} ~ {intersection_end_date} 기간의 ")
                        print(f"데이터가 크롤 및 UPDATE 되었습니다. UPDATE COUNT: {update_count} ")
                    else:
                        print(f"intersection {intersection_start_date} ~ {intersection_end_date} intersection 기간")
                        print("의 데이터에 대해서 UPDATE가 정상적으로 이루어 지지 않음")
                else:
                    print("hardmode 가 아닙니다.")
                    pass

                # unique 기간에대해서는 INSERT

                unique_req = request_time_interval - intersection
                unique_db = db_time_interval - intersection
                unique_req_as_list = list(unique_req)
                unique_req_as_list.sort()

                if (len(unique_req) == 0):
                    # 이미 intersection 에서 처리됨. 그러므로 pass함.
                    # case1: DB기간안에 start~end가 포함되는 경우
                    # case1: DB기간과 start-end가 같은경우
                    if (hard_mode == False):
                        print("hard_mode 설정이 False 이고")
                        print("요청하신 기간의 데이터는 이미 DB에 있으니")
                        print("아무런 작업이 일어나지 않습니다.")
                        return True
                    else:
                        print("update_only")
                        update_only = True
                    # insert_okay = False
                    pass
                elif ((len(unique_req) > 0) and (start_date < oldest_date_indb) and (lastest_date_indb < end_date) \
                      and (len(unique_db) == 0)):
                    print("왼쪽 오른쪽 둘 다 있습니다.")
                    #print(f"len(unique_req):{len(unique_req)}")
                    #print(f"start_date:{start_date}")
                    #print(f"oldest_date_indb:{oldest_date_indb}")
                    #print(f"start_date < oldest_date_indb: {start_date < oldest_date_indb}")
                    #print(f"lastest_data_indb: {lastest_date_indb}")
                    #print(f"end_data: {end_date}")
                    #print(f"lastest_date_indb < end_date: {lastest_date_indb < end_date}")
                    #print(f"len(unique_db) == 0): {len(unique_db) == 0}")

                    # start_date~end_date 가 DB기간을 포함하면서 더 넓은 경우, 최대 양쪽 끝 두 범위가 나올 수 있음.
                    # ex: 처음부터 작업을 하려고한다.
                    # 장중에 hist_crawler.save_history_of('우리은행', hard_mode=True)를 한 번돌려서 저장을 한다음
                    # hist_crawler.save_history_of('우리은행', hard_mode=True) 다시 한번 돌리는 경우 이런케이스가 발생하게된다.

                    # 이런 경우는 드물 것이라고 예상.
                    # 양쪽 다 나온 경우 : 왼쪽, 오른쪽 unique 범위에대해서 처리.
                    #print("unique_req")
                    #print(unique_req)
                    #print("unique_db")
                    #print(unique_db)
                    if(not ignore_past):
                        print("과거를 무시합니다.")
                        left_side = [date for date in unique_req_as_list if date < intersection_start_date]
                        left_side_start = left_side[0]
                        left_side_end = left_side[-1]

                        data_bomb = self.base_crawl_method_for_history(company_name, left_side_start.strftime("%Y-%m-%d")
                                                                  , left_side_end.strftime("%Y-%m-%d"))
                        left_okay, left_count = self.toss_databomb_to_db(data_bomb, update_mode=False)
                        if (left_okay):
                            print(f"{left_side_start} ~ {left_side_end} 기간의 ")
                            print(f"데이터가 크롤 및 INSERT 되었습니다. INSERT COUNT: {left_count} ")
                        else:
                            print(f"{left_side_start} ~ {left_side_end} 기간의 ")
                            print(f"데이터가 정상적으로 크롤 및 INSERT 되지 않았습니다. INSERT COUNT: {left_count} ")
                    else:
                        left_okay = True
                        left_count = 0

                    right_side = [date for date in unique_req_as_list if date > intersection_end_date]
                    right_side_start = right_side[0]
                    right_side_end = right_side[-1]

                    data_bomb = self.base_crawl_method_for_history(company_name, right_side_start.strftime("%Y-%m-%d")
                                                              , right_side_end.strftime("%Y-%m-%d"))
                    right_okay, right_count = self.toss_databomb_to_db(data_bomb, update_mode=False)
                    if (right_okay):
                        print(f"{right_side_start} ~ {right_side_end} 기간의 ")
                        print(f"데이터가 크롤 및 INSERT 되었습니다. INSERT COUNT: {right_count} ")
                    else:
                        print(f"{right_side_start} ~ {right_side_end} 기간의 ")
                        print(f"데이터가 정상적으로 크롤 및 INSERT 되지 않았습니다. INSERT COUNT: {right_count} ")

                    insert_okay = left_okay * right_okay
                    insert_count = left_count + right_count

                else:
                    #
                    # 왼쪽 또는 오른쪽만 나온 경우
                    print("왼쪽 또는 오른쪽만 나온 경우이군요.")
                    unique_req_start_date = unique_req_as_list[0]
                    unique_req_end_date = unique_req_as_list[-1]

                    if((unique_req_end_date < lastest_date_indb) and ignore_past):
                        # 왼쪽만 나오고 , ignore_past=True인 경우
                        print("왼쪽만 나오고, ignore_past인경우이군요.")
                        only_ignore_past=True
                        pass
                    else:
                        data_bomb = self.base_crawl_method_for_history(company_name, unique_req_start_date.strftime("%Y-%m-%d")
                                                                  , unique_req_end_date.strftime("%Y-%m-%d"))
                        insert_okay, insert_count = self.toss_databomb_to_db(data_bomb, update_mode=False)
                        if (insert_okay):
                            print(f"{unique_req_start_date} ~ {unique_req_end_date} 기간의 ")
                            print(f"데이터가 크롤 및 INSERT 되었습니다. INSERT COUNT: {insert_count} ")
                        else:
                            print(f"{unique_req_start_date} ~ {unique_req_end_date} 기간의 ")
                            print(f"데이터가 정상적으로 크롤 및 INSERT 되지 않았습니다. INSERT COUNT: {insert_count} ")


            # 두 기간의 intersection이 없을 때
            else:
                # start_date~end_date기간의 데이터에 대해서 INSERT만 일어나면된다.
                data_bomb = self.base_crawl_method_for_history(company_name, start_date.strftime("%Y-%m-%d")
                                                          , end_date.strftime("%Y-%m-%d"))
                insert_okay, insert_count = self.toss_databomb_to_db(data_bomb, update_mode=False)
                if (insert_okay):
                    print("DB기간과 겹치는 기간이 없었습니다.")
                    print(f"{start_date} ~ {end_date} 기간의 ")
                    print(f"데이터가 크롤 및 저장 되었습니다 INSERT COUNT: {insert_count}")
                else:
                    print("DB기간과 겹치는 기간이 없었습니다.")
                    print("하지만")
                    print(f"{start_date} ~ {end_date} 기간의 ")
                    print("데이터가 정상적으로 INSERT가 이루어 지지 않았습니다.")

            # INSERT 마무리
            if (insert_okay and not update_only):
                print(f"총 {insert_count}개의 데이터가 크롤 및 INSERT 되었습니다. INSERT COUNT: {insert_count} ")
                return True
            elif (update_only):
                print("ONLY UPDATE")
                return True
            elif (only_ignore_past):
                print("ONLY IGNORE_PAST")
                return True
            else:
                return False


        # DB에 데이터가 없을때 start_date~end_date에 대해서 INSERT
        else:
            # DB에 데이터가 없을 때는
            #
            # hard_mode인지 아닌지가 필요 없다.
            print(f"DB에 {company_name}의 데이터가 아무것도 없습니다.")

            data_bomb = self.base_crawl_method_for_history(company_name, start_date.strftime("%Y-%m-%d")
                                                      , end_date.strftime("%Y-%m-%d"))
            insert_okay, insert_count = self.toss_databomb_to_db(data_bomb, update_mode=False)

            if (insert_okay):
                oldest_rp = self.db.db_execute(oldest_date_indb_query)
                lastest_rp = self.db.db_execute(lastest_date_indb_query)

                if(oldest_rp.rowcount):
                    oldest_date_indb = oldest_rp.first()[0]
                    lastest_date_indb = lastest_rp.first()[0]
                else:
                    print(f"{company_name}({code})는 더이상 유효한 종목이 아닌거 같습니다?")
                    return False

                print(f"{oldest_date_indb} ~ {lastest_date_indb} 기간의 ")
                print(f"INSERT COUNT: {insert_count} 데이터가 크롤 및 저장 되었습니다.")
                return True
            else:

                print("하지만")
                print(f"{start_date} ~ {end_date} (요청)기간의 ")
                print("데이터가 정상적으로 INSERT가 이루어 지지 않았습니다.")
                return False
