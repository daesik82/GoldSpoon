"""

"""
from datetime import datetime
from sqlalchemy import (MetaData, Table, Column, Integer, String, BigInteger, Float,
                        DateTime, ForeignKey)

__all__ = ['__meta', 'stocks_table', 'hist_data_table', 'company_info_table']

__meta = MetaData()

stocks_table = Table('stocks', __meta,
                     #Column('id', Integer(), primary_key=True),
                     Column('code', String(100), primary_key=True),
                     Column('company', String(100), index=True, unique=True, nullable=False),
                     Column('market_type', Integer()),
                     Column('main_ics', String(100), nullable=True),
                     Column('alt_ics', String(100), nullable=True),
                     Column('created_on', DateTime(), default=datetime.now),
                     Column('updated_on', DateTime(), default=datetime.now, onupdate=datetime.now),
                     Column('delisted_on', DateTime(), nullable=True)
                    )


hist_data_table = Table('hist_data', __meta,
                        Column('id', BigInteger(), primary_key=True),
                        Column('code', ForeignKey('stocks.code')),
                        Column('date', DateTime(), index=True),
                        Column('close', Integer()),
                        Column('open', Integer()),
                        Column('high', Integer()),
                        Column('low', Integer()),
                        Column('volume', Integer())
                       )


company_info_table = Table('company_info', __meta,
                        Column('id', BigInteger(), primary_key=True),
                        Column('code', ForeignKey('stocks.code')),
                        Column('year', String(100), index=True),
                        Column('total_sales', Float()),
                        Column('domestic_sales', Float()),
                        Column('export_sales', Float()),
                        Column('cogs', Float()),
                        Column('gross_profit', Float()),
                        Column('operating_income', Float()),
                        Column('extraordinary_items_income', Float()),
                        Column('cont_operating_income', Float()),
                        Column('net_income', Float()),
                        Column('ebitda', Float()),
                        Column('ebit', Float()),
                        Column('total_assets', Float()),
                        Column('current_assets', Float()),
                        Column('inventories', Float()),
                        Column('non_current_assets', Float()),
                        Column('tangible_assets', Float()),
                        Column('intangible_assets', Float()),
                        Column('book_value_1', Float()),
                        Column('book_value_2', Float()),
                        Column('total_liabilities', Float()),
                        Column('current_liabilities', Float()),
                        Column('short_term_borrowings', Float()),
                        Column('non_current_liabilities', Float()),
                        Column('long_term_borrowings', Float()),
                        Column('capital_expenditure', Float()),
                        Column('total_equity', Float()),
                        Column('equity_owers', Float()),
                        Column('stock_issued', Float()),
                        Column('cashflow_operations', Float()),

                       )
#Column('created_on', DateTime(), default=datetime.now),
#                     Column('updated_on', DateTime(), default=datetime.now, onupdate=datetime.now),
# hist_data_table 에 추가

#from sqlalchemy import Index
#Index('ix_test', mytable.c.cookie_sku, mytable.c.cookie_name))