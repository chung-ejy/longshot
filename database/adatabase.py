from pymongo import MongoClient, DESCENDING
import pandas as pd
from database.idatabase import IDatabase
import asyncio
import os
from dotenv import load_dotenv
load_dotenv()
token = os.getenv("MONGO")
import certifi
ca = certifi.where()

class ADatabase(IDatabase):
    
    def __init__(self,name):
        self.name = name
        super().__init__()
    
    def connect(self):
        self.client = MongoClient("localhost",27017)
    
    def cloud_connect(self):
        self.client = MongoClient(f"mongodb+srv://chungejy:{token}@scene.zblsh.mongodb.net/myFirstDatabase?retryWrites=true&w=majority",tlsCAFile=ca)
    
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
            data = table.find({},{"_id":0},show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(self.name,table_name,str(e))
    
    def query(self,table_name,query):
        try:
            db = self.client[self.name]
            table = db[table_name]
            data = table.find(query,{"_id":0},show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(self.name,table_name,str(e))
    
    def update(self,table_name,query,update):
        try:
            db = self.client[self.name]
            table = db[table_name]
            data = table.find_one_and_update(query,{"$set":query})
        except Exception as e:
            print(self.name,table_name,str(e))

    def delete(self,table_name,query):
        try:
            db = self.client[self.name]
            table = db[table_name]
            data = table.delete_many(query)
        except Exception as e:
            print(self.name,table_name,str(e))
