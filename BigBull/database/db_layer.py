"""
"""
from datetime import datetime

import pandas as pd

from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError

from sqlalchemy import and_
from sqlalchemy import select, desc, update
from sqlalchemy.sql.expression import bindparam

__all__ = ['StockDB']


def add_target_item(dic, col_name):
    dic['target_'+col_name] = dic.get(col_name)
    return dic


class BaseDBAccessLayer:
    """
    This is base access layer for DB
    """
    def __init__(self, conn_string, meta):
        """
        TDL:
        1. meta 없으면 오류

        :param conn_string:
        :param meta:
        """
        self.__engine = create_engine(conn_string, pool_recycle=3600, encoding='utf-8')
        self.__meta = meta # alembic 아마도 사용?
        self.__meta.create_all(self.__engine)
        self.__connection = self.__engine.connect()

    def db_execute(self, statement):
        """

        :param statement:
        :return:
        """
        return self.__connection.execute(statement)

    def find_table(self, table_name):
        # DB 에서 처리하는 로직? or meta에서 처리하는 로직
        try:
            table = self.__meta.tables[table_name]
        except KeyError:
            print(f"There isn't '{table_name}' table in DB")

        return table

    def select_columns(self, table_name, columns=[]):
        try:
            if(type(columns) != list):
                raise TypeError

            table = self.find_table(table_name)

            if(len(columns)==0):
                s = table.select()
                rp = self.__connection.execute(s)
                return rp

            else:
                # 원하는 column 만 선택해서 조회
                column_list = []
                for column in columns:
                    if(table.columns.has_key(column)):
                        column_list.append(table.columns.get(column))
                    else:
                        print(f"{table_name} table 에는 {column} Column 이 없습니다.")

                s = select(column_list)
                rp = self.__connection.execute(s)
                return rp

        except TypeError as error:
            print("argument 'columns' 는 'list' 입니다.")

    def insert_multiple(self, table_name, data_dict_list):
        """
        Insert multiple rows in a table you select

        :param table_name: string, table name
        :param data_dict_list: list of dict, data which is inserted in to table
        :return:
        """
        transaction = self.__connection.begin()

        try:
            if (type(data_dict_list) != list):
                print("왜 type 에러일까?")
                print(type(data_dict_list))
                print(data_dict_list)
                raise TypeError
            if (data_dict_list):
                table = self.find_table(table_name)
                ins = table.insert()
                result = self.__connection.execute(ins,
                                                   data_dict_list)
                transaction.commit()
                return True
            else:
                print("This is empty dict")
                transaction.rollback()
                return False

        except TypeError as error:
            input_type = type(data_dict_list)
            print(f"argument 'data_dict_list' 는 {input_type}가 아니라 'list' 입니다.")
            transaction.rollback()
            return False

        except IntegrityError as error:
            error_code, error_msg = error.orig.args
            print(f"IntergrityError:\n"
                  f"\t-error_code: {error_code}\n"
                  f"\t-error_msg: {error_msg}")
            print("**** RollBack ****")
            transaction.rollback()

            return False

    def update_multiple(self, table_name, column, data_dict_list):
        """
        하나의 column에 대해서 조건을 줘서
        DB에 기존에 있던 데이터에 대해서 한 번에 Update 한다.
        """
        transaction = self.__connection.begin()  # self.__connection.begin()

        if (data_dict_list):
            try:
                table = self.find_table(table_name)
                if (table.c.has_key(column)):
                    c = table.c.get(column)

                    upd = table.update(). \
                        where(c == bindparam('target_' + column))

                    added_data_dict_list = [add_target_item(dic, column) for dic in data_dict_list]
                    # print("added_data_dict_list: ")
                    # print(added_data_dict_list)
                    result = self.__connection.execute(upd, added_data_dict_list)
                    transaction.commit()
                    return True

                else:
                    print("{} table 에는 {} Column 이 없습니다".format(table_name, column))
                    print("Let's RollBack")
                    transaction.rollback()
                    return False

            except IntegrityError as error:
                error_code, error_msg = error.orig.args

                print("IntegrityError!!")
                print(">>>error_code: {}".format(error_code))
                print(">>>error_msg: {}".format(error_msg))
                print("Let's RollBack")
                transaction.rollback()

                return False

        else:
            print("빈 dict입니다")
            transaction.rollback()
            print("Let's RollBack")
            return False


class StockDB(BaseDBAccessLayer):
    def __init__(self, conn_string, meta, stocks_db_name='stocks'):
        super().__init__(conn_string, meta)
        self.stocks_db_name = stocks_db_name

    def find_stock_code(self, company_name):
        """
        회사의 주가 종목코드를 DB에서 찾아서 가져오는 함수
        """
        table = self.find_table(self.stocks_db_name)
        code_column = table.c.get('code')
        company_name_column = table.c.get('company')

        s = select([code_column]).where(company_name_column == company_name)

        rp = self.db_execute(s)
        if(rp.rowcount):
            result = rp.first()
            return result[0]
        else:
            print(f"{company_name} 회사는 DB에 없습니다.")

    def load_all_stock_code(self):
        """
        DB에서 코스닥 시장과 코스피 시장의 (회사명, 종목코드) 를 가져오는 함수
        :return:
        """
        rp = self.select_columns(self.stocks_db_name)

        for row in rp:
            yield (row.company, row.code)

    def get_history_from_a_to_b(self, table_name, company_name, start_date, end_date,
                                      columns=[], dataframe=False):

        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.strptime(end_date, "%Y-%m-%d")

        stock_code = self.find_stock_code(company_name)

        copies = columns.copy()

        try:
            if(type(columns) != list):
                raise TypeError

        except TypeError as error:
            print("argument 'columns' 는 list type 입니다.")


        table = self.find_table(table_name)

        if(len(columns) == 0):
            s = select([table]).order_by(table.c.date)
        else:
            # 원하는 column 만 선택해서 조회
            column_list = []
            if not 'date' in columns:
                copies.append('date')

            for column in copies:
                if (table.c.has_key(column)):
                    column_list.append(table.c.get(column))
                else:
                    print(f"{table_name} table 에는 {column} Column 이 없습니다.")

            s = select(column_list).order_by(table.c.date)

        s = s.where(
            and_(
                table.c.date >= start_date,
                table.c.date <= end_date,
                table.c.code == stock_code
            )
        )

        if(dataframe):
            return pd.read_sql(s, self.__engine, index_col="date", parse_dates=["date"])

        rp = self.__connection.execute(s)

        return rp
