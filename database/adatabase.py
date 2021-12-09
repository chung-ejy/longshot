from pymongo import MongoClient, DESCENDING
import pandas as pd
from database.idatabase import IDatabase
import asyncio
class ADatabase(IDatabase):
    
    def __init__(self,name,client):
        self.name = name
        self.client = client
        super().__init__()
    
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
            data = table.find({},{"_id":0},show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(self.name,table_name,str(e))

    def delete(self,table_name,query):
        try:
            db = self.client[self.name]
            table = db[table_name]
            data = table.delete_many(query)
        except Exception as e:
            print(self.name,table_name,str(e))
