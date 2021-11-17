from pymongo import MongoClient, DESCENDING
import pandas as pd
from strategy.istrategy import IStrategy
from database.strategy import Strategy
class AStrategy(IStrategy):
    def __init__(self,name,subscriptions,timeperiod):
        self.name = name
        self.subscriptions = subscriptions
        self.timeperiod = timeperiod
        self.db = Strategy(name)
        super().__init__()
    
    def load(self):
        for subscription in subscriptions:
            self.subscription = DBFact.subscribe(subscription)
    
    def create_training_set(self):
        return pd.DataFrame([{"test":test}])
    
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