from database.adatabase import ADatabase
import pandas as pd
class Strategy(ADatabase):
    
    def __init__(self,name):
        super().__init__(name)
    
    def retrieve_sim(self,params):
        try:
            db = self.client[self.name]
            table = db["sim"]
            data = table.find(params,{"_id":0},show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(str(e))
    
    def transform_check(self,query):
        try:
            db = self.client[self.name]
            table = db["transformed"]
            data = table.find(query,{"_id":0,"ticker":1,"date":1},show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(str(e))