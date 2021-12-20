from pymongo import MongoClient, DESCENDING
import pandas as pd
from strategy.istrategy import IStrategy
from database.strategy import Strategy
from database.dbfact import DBFact
from tqdm import tqdm

class AStrategy(IStrategy):
    def __init__(self,name,start_date,end_date,subscriptions,modeling_params,trading_params):
        self.name = name
        self.start_date = start_date
        self.end_date = end_date
        self.subscriptions = subscriptions
        self.modeling_params = modeling_params
        self.trading_params = trading_params
        self.db = Strategy(name)
        self.subscribed = False
        self.loaded = False
        self.db.connect()
        self.simmed = self.db.retrieve_sim(self.modeling_params).index.size > 0 
        self.db.disconnect()
        self.ai = False
        super().__init__()
    
    def subscribe(self):
        for subscription in self.subscriptions:
            self.subscriptions[subscription]["db"] = DBFact.subscribe(subscription)
        self.subscribed = True
  
    def create_sim(self):
        if self.simmed:
            self.db.connect()
            sim = self.db.retrieve("sim")
            self.db.disconnect()
        else:
            sim = pd.DataFrame([{"test":test}])
            self.simmed = True
        return sim