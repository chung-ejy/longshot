from datetime import datetime
import pandas as pd
import requests
from dotenv import load_dotenv
import os
load_dotenv()
token = os.getenv("TIINGO")

class TiingoExtractor(object):

    @classmethod
    def extract(self,ticker,start,end):
        try:
            headers = {
                "Content-Type":"application/json"
            }

            params = {
                "token":token,
                "startDate":start,
                "endDate":end
            }
            url = f"https://api.tiingo.com/tiingo/daily/{ticker}/prices"
            requestResponse = requests.get(url,headers=headers,params=params)
            return pd.DataFrame(requestResponse.json())
        except Exception as e:
            print(str(e))

