from database.adatabase import ADatabase
import pandas as pd
class Strategy(ADatabase):
    
    def __init__(self,name,client):
        super().__init__(name,client)
    
    def retrieve_sim(self,params):
        try:
            db = self.client[self.name]
            table = db["sim"]
            data = table.find(params,{"_id":0},show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(str(e))