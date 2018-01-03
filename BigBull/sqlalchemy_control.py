# sqlAlchemy 설정 및 맵핑 관련

# Importing packages
from datetime import datetime
import time
import random

# sqlalchemy packages
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, Numeric, String, DateTime, ForeignKey
from sqlalchemy.orm import sessionmaker, relationship, backref
from sqlalchemy.ext.declarative import declarative_base

db_string = "postgresql://daesik:@localhost/db_piotroski"

engine = create_engine(db_string, echo=True)

Session = sessionmaker(bind=engine)
session = Session()

Base = declarative_base()


# Mapping
class Stock(Base):
    __tablename__ = 'stocks'

    stock_id = Column(Integer, primary_key=True)
    stock_code = Column(String, unique=True, nullable=False, primary_key=True)
    company = Column(String, index=True, unique=True, nullable=False)
    market_type = Column(Integer)
    m_ics = Column(String)
    w_ics = Column(String)
    created_on = Column(DateTime, default=datetime.utcnow)
    updated_on = Column(DateTime, default=datetime.utcnow)

    base_param = relationship('BaseParam', back_populates='stock')
    bookmarket_param = relationship('BookMarketParam', back_populates='stock')

    # def __repr__(self):
    #    return "<Stock ==> id : {0}, code : {1}, company : {2}, market_type : {3}, created : {4}, updated : {5}>".format(self.stock_id,
    #                                                                                                                     self.stock_code,
    #                                                                                                                     self.company,
    #                                                                                                                     self.market_type,
    #                                                                                                                     self.created_on,
    #                                                                                                                     self.updated_on,)


class BaseParam(Base):
    __tablename__ = 'base_params'

    baseparam_id = Column(Integer, primary_key=True)
    stock_code = Column(String, ForeignKey('stocks.stock_code'))
    date = Column(DateTime)
    price_open = Column(Integer)
    price_close = Column(Integer)
    price_high = Column(Integer)
    price_low = Column(Integer)
    quant = Column(Integer)
    market_sum = Column(Integer)

    stock = relationship("Stock", back_populates="base_param")


class BookMarketParam(Base):
    __tablename__ = 'bookmarket_params'

    bookmarketparam_id = Column(Integer, primary_key=True)
    stock_code = Column(String, ForeignKey('stocks.stock_code'))
    listed_stock_cnt = Column(Integer)
    property_total = Column(Integer)
    debt_total = Column(Integer)
    pbr = Column(Integer)

    stock = relationship('Stock', back_populates='bookmarket_param')

Base.metadata.create_all(engine)