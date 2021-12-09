from database.adatabase import ADatabase
import pandas as pd

class PortfolioDb(ADatabase):
    
    def __init__(self,name):
        super().__init__(name)
    
    def retrieve_trades(self,params):
        try:
            db = self.client[self.name]
            table = db["trades"]
            data = table.find(params,{"_id":0},show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(str(e))