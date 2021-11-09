from database.adatabase import ADatabase
import pandas as pd
class Strategy(ADatabase):
    
    def __init__(self):
        super().__init__("project_strategy")

    
    def retrieve_hlt_data(self,ticker):
        try:
            db = self.client[self.name]
            table = db["high_level_sim"]
            data = table.find({"ticker":ticker},{"_id":0},show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(str(e))
    
    def clear_trades(self,tw,wcty,wmty):
        try:
            db = self.client[self.name]
            table = db["trades"]
            data = table.delete_many({"tw":tw,"wcty":wcty,"wmty":wmty})
        except Exception as e:
            print(str(e))