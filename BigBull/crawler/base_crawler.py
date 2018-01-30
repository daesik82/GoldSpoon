"""

"""
from datetime import datetime

from bs4 import BeautifulSoup
import requests


class BaseCrawler:
    def __init__(self, db):
        self.db = db
        self.__start_time = 0
        self.__end_time = 0

    def make_soup(self, url, parser='html.parser'):
        req = requests.get(url)
        html = req.text

        return BeautifulSoup(html, parser)

    def logging(self):
        pass

    def get_timetaken(self):
        pass