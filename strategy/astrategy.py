from pymongo import MongoClient, DESCENDING
import pandas as pd
from strategy.istrategy import IStrategy
from database.strategy import Strategy
from database.dbfact import DBFact
class AStrategy(IStrategy):
    def __init__(self,name,subscriptions,timeperiod):
        self.name = name
        self.subscriptions = subscriptions
        self.timeperiod = timeperiod
        self.db = Strategy(name)
        self.subscribed = False
        super().__init__()
    
    def subscribe(self):
        for subscription in self.subscriptions:
            self.subscriptions[subscription]["db"] = DBFact.subscribe(subscription)
        self.subscribed = True

    def load(self):
        if not self.subscribed:
            self.subscribe()
        else:
            for subscription in self.subscriptions:
                db = self.subscriptions[subscription]["db"]
                table = self.subscriptions[subscription]["table"]
                db.connect()
                self.subscriptions[subscription]["dataset"] = db.retrieve(table)
                db.disconnect()
        
    def create_training_set(self):
        return pd.DataFrame([{"test":test}])
    
    def create_prediction_set(self):
        return pd.DataFrame([{"test":test}])
    
    def create_sim(self):
        return pd.DataFrame([{"test":test}])
    
    def create_record(self):
        return {
                "name":self.name
                ,"subscriptions":self.subscriptions
                ,"timeperiod":self.timeperiod
                }
                
    def daily_recommendation(self):
        return pd.DataFrame([{"test":test}])