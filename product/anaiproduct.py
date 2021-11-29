from pymongo import MongoClient, DESCENDING
import pandas as pd
from product.iproduct import IProduct
from database.product import Product
from database.dbfact import DBFact
import pandas as pd
from product.aproduct import AProduct

class AnAIProduct(AProduct):
    def __init__(self,name,subscriptions,params):
        super().__init__(name,
                        subscriptions,
                        params)
        self.db.connect()
        self.transformed = self.db.retrieve_transformed(params).index.size > 0
        self.db.disconnect()

    def initial_transform(self):
        return pd.DataFrame([{"test":test}])
    
    def create_training_set(self,dataset,year,quarter):
        return pd.DataFrame([{"test":test}])
    
    def create_prediction_set(self):
        return pd.DataFrame([{"test":test}])
    