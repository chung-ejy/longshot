from pymongo import MongoClient, DESCENDING
import pandas as pd
from strategy.istrategy import IStrategy
from database.strategy import Strategy
from database.dbfact import DBFact
import pandas as pd
from strategy.astrategy import AStrategy

class AnAIStrategy(AStrategy):
    def __init__(self,name,start_date,end_date,subscriptions,params):
        super().__init__(name,start_date,end_date,subscriptions,params)
        self.ai = True