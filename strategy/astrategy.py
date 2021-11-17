from pymongo import MongoClient, DESCENDING
import pandas as pd
from strategy.istrategy import IStrategy
from database.strategy import Strategy
from database.dbfact import DBFact
class AStrategy(IStrategy):
    def __init__(self,name,start_date,end_date,subscriptions):
        self.name = name
        self.start_date = start_date
        self.end_date = end_date
        self.subscriptions = subscriptions
        self.db = Strategy(name)
        self.subscribed = False
        self.loaded = False
        self.db.connect()
        self.simmed = self.db.retrieve("sim").index.size > 0 
        self.db.disconnect()
        super().__init__()
    
    def subscribe(self):
        for subscription in self.subscriptions:
            self.subscriptions[subscription]["db"] = DBFact.subscribe(subscription)
        self.subscribed = True

    def load(self):
        if self.simmed:
            self.loaded = True
        else:
            if not self.subscribed:
                self.subscribe()
            else:
                for subscription in self.subscriptions:
                    db = self.subscriptions[subscription]["db"]
                    table = self.subscriptions[subscription]["table"]
                    db.connect()
                    self.subscriptions[subscription]["dataset"] = db.retrieve(table)
                    db.disconnect()
            self.loaded = True
    
    def create_record(self):
        return {
                "name":self.name
                ,"subscriptions":[{subscription:{"table":self.subscriptions[subscription]["table"]}} for subscription in self.subscriptions]
                ,"timeperiod":self.timeperiod
                }
    
    def create_sim(self):
        if self.simmed:
            self.db.connect()
            sim = self.db.retrieve("sim")
            self.db.disconnect()
        else:
            sim = pd.DataFrame([{"test":test}])
        return sim
               
    def daily_recommendation(self):
        return pd.DataFrame([{"test":test}])