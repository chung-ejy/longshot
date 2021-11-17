from pymongo import MongoClient, DESCENDING
import pandas as pd
from strategy.istrategy import IStrategy
from database.strategy import Strategy
from database.dbfact import DBFact
import pandas as pd
from strategy.astrategy import AStrategy

class AnAIStrategy(AStrategy):
    def __init__(self,start_date,end_date):
        super().__init__("daily_btc_rolling_percent",
                            start_date,
                            end_date,
                        {"market":{"table":"crypto"
                                    }})

    # def create_sim(self):
    #     start_year = start_date.year
    #     end_year = end_date.year
    #     prices = self.subscriptions["market"]["dataset"].copy()
    #     prices["date"] = pd.to_datetime(prices["date"])
    #     prices["year"] = [x.year for x in prices["date"]]
    #     prices["quarter"] = [x.quarter for x in prices["date"]]
    #     for year in range(start_year,end_year+1):
    #         for quarter in range(1,5):
    #             ts = self.create_training_set(prices,year,quarter)
    #     return pd.DataFrame([{"test":test}])

    # def create_training_set(self,dateset,year,quarter):
    #     return pd.DataFrame([{"test":test}])
    
    # def create_prediction_set(self):
    #     return pd.DataFrame([{"test":test}])
               
    # def daily_recommendation(self):
    #     return pd.DataFrame([{"test":test}])

    
    # def create_record(self):
    #     return {
    #             "name":self.name
    #             ,"subscriptions":[{subscription:{"table":self.subscriptions[subscription]["table"]}} for subscription in self.subscriptions]
    #             ,"timeperiod":self.timeperiod
    #             }
    
    # def create_sim(self):
    #     return pd.DataFrame([{"test":test}])
               
    # def daily_recommendation(self):
    #     return pd.DataFrame([{"test":test}])