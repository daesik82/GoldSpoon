# Ex)
```python
import BigBull

# 'conn_string' is a connection url for connecting to your databse
# So, you have to install you own db before using this library.
# more information plz, check http://docs.sqlalchemy.org/en/latest/core/engines.html
# For example, if you use PostgreSQL for db
# url = 'postgresql+psycopg2://username:password@localhost:5432/mydb'

conn_string = "mysql+pymysql://root:deepstock@localhost/deepstock?charset=utf8"
db = BigBull.StockDB(connstring, BigBull.__meta)
crawler = BigBull.StockCodeCrawler(db)

examples = crawler.crawl_stock_code(BigBull.KOSPI)
print(examples.count()) #1406

# Saving All stock codes in both kospi and kosdak
cralwer.save_stock_code()
```
