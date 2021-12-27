from database.adatabase import ADatabase
import pandas as pd
class SEC(ADatabase):
    
    def __init__(self):
        super().__init__("sec")

    def retrieve_num_data(self,adsh):
        try:
            db = self.client[self.name]
            table = db["nums"]
            data = table.find({"adsh":adsh},{"_id":0},show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(str(e))

    def retrieve_filing_data(self,cik):
        try:
            db = self.client[self.name]
            table = db["filings"]
            data = table.find({"cik":cik},{"_id":0},show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(str(e))
    
    def retrieve_adshs(self):
        try:
            db = self.client[self.name]
            table = db["filings"]
            data = table.find({},{"_id":0,"adsh":1},show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(str(e))