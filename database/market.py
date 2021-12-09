from database.adatabase import ADatabase
import pandas as pd
class Market(ADatabase):
    
    def __init__(self):
        super().__init__("project_market")

    def retrieve_tickers(self,currency):
        try:
            db = self.client[self.name]
            table = db[currency]
            data = table.find({},{"ticker":1,"_id":0},show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(str(e))    

    def retrieve_ticker_prices(self,currency,ticker):
        try:
            db = self.client[self.name]
            table = db[currency]
            data = table.find({"ticker":ticker},{"_id":0},show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(str(e))
    
    def retrieve_date_range(self):
        try:
            db = self.client[self.name]
            table = db["prices"]
            data = table.find({},{"date":1,"_id":0},show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(str(e))