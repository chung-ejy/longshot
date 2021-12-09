from database.adatabase import ADatabase
import pandas as pd
class Product(ADatabase):
    
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
    
    def retrieve_transformed(self,params):
        try:
            db = self.client[self.name]
            table = db["transformed"]
            data = table.find(params,{"_id":0},show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(str(e))