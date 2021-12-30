from pymongo import MongoClient, DESCENDING
import pandas as pd
from strategy.istrategy import IStrategy
from database.strategy import Strategy
from database.dbfact import DBFact
import pandas as pd
from strategy.astrategy import AStrategy

class AnAIStrategy(AStrategy):
    def __init__(self,name,start_date,end_date,subscriptions,modeling_params,trading_params):
        super().__init__(name,start_date,end_date,subscriptions,modeling_params,trading_params)
        self.ai = True
    
    def ismodeled(self):
        params = self.modeling_params
        params["year"] = datetime.now().year
        params["month"] = (datetime.now().month - 1) // 3 + 1
        self.db.connect()
        models = self.db.query("models",params)
        self.db.disconnect()
        return models.index.size > 0