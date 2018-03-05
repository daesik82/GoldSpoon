"""

"""
from datetime import datetime
from sqlalchemy import (MetaData, Table, Column, Integer, String, BigInteger,
                        DateTime, ForeignKey)

__all__ = ['__meta', 'stocks_table', 'hist_data_table']

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


#Column('created_on', DateTime(), default=datetime.now),
#                     Column('updated_on', DateTime(), default=datetime.now, onupdate=datetime.now),
# hist_data_table 에 추가

#from sqlalchemy import Index
#Index('ix_test', mytable.c.cookie_sku, mytable.c.cookie_name))