from pymongo import MongoClient, DESCENDING
import pandas as pd
from product.iproduct import IProduct
from database.product import Product
from database.dbfact import DBFact
class AProduct(IProduct):
    def __init__(self,name,subscriptions,params):
        self.name = name
        self.subscriptions = subscriptions
        self.params = params
        self.db = Product(name)
        self.subscribed = False
        self.loaded = False
        self.db.connect()
        self.simmed = self.db.retrieve_sim(params).index.size > 0 
        self.db.disconnect()
        super().__init__()
    
    def subscribe(self):
        for subscription in self.subscriptions:
            self.subscriptions[subscription]["db"] = DBFact.subscribe(subscription)
        self.subscribed = True

    def load(self):
        if self.simmed:
            self.loaded = True
        else:
            if not self.subscribed:
                self.subscribe()
            else:
                for subscription in self.subscriptions:
                    if self.subscriptions[subscription]["preload"]:
                        db = self.subscriptions[subscription]["db"]
                        for table in self.subscriptions[subscription]["tables"]:
                            db.connect()
                            self.subscriptions[subscription]["tables"][table] = db.retrieve(table)
                            db.disconnect()
                    else:
                        continue
            self.loaded = True
    
    def create_record(self):
        return {
                "name":self.name
                # ,"subscriptions":[{subscription:{"table":self.subscriptions[subscription]["table"]}} for subscription in self.subscriptions]
                # ,"timeperiod":self.timeperiod
                }
    
    def create_sim(self):
        if self.simmed:
            self.db.connect()
            sim = self.db.retrieve("sim")
            self.db.disconnect()
        else:
            sim = pd.DataFrame([{"test":test}])
        return sim