from pymongo import MongoClient, DESCENDING
import pandas as pd
from portfolio.iportfolio import IPortfolio
from database.portfoliodb import PortfolioDb

class APortfolio(IPortfolio):
    
    def __init__(self,name):
        self.name = name
        self.db = PortfolioDb(name)
        super().__init__()

    def load():
        self.strats = []
    
    def sim():
        return pd.DataFrame([{}])
    
    def backtest():
        return pd.DataFrame([{}])