"""
설명
"""
KOSPI = 0
KOSDAK = 1


from .database import *
from .crawler import *
from .data_structure import *

__all__ = [database.__all__, crawler.__all__, data_structure.__all__]

