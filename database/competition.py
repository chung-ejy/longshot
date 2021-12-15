from database.strategy import Strategy
import pandas as pd
class Competition(Strategy):
    
    def __init__(self,name):
        super().__init__(name)
    
    def retrieve_transformed(self,ticker):
        try:
            db = self.client[self.name]
            table = db["transformed"]
            data = table.find({"ticker":ticker},{"_id":0},show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(str(e))