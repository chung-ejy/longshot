from pymongo import MongoClient, DESCENDING
import pandas as pd
from database.idatabase import IDatabase

class ADatabase(IDatabase):
    
    def __init__(self,name):
        self.name = name
        super().__init__()
    
    def connect(self):
        self.client = MongoClient("localhost",27017)
    
    def disconnect(self):
        self.client.close()

    def store(self,table_name,data):
        try:
            db = self.client[self.name]
            table = db[table_name]
            records = data.to_dict("records")
            table.insert_many(records)
        except Exception as e:
            print(self.name,table_name,str(e))
    
    def retrieve(self,table_name):
        try:
            db = self.client[self.name]
            table = db[table_name]
            data = table.find(show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(self.name,table_name,str(e))
