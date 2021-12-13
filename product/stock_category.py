import pandas as pd
from product.aproduct import AProduct
from processor.processor import Processor as p
from datetime import timedelta
import pytz
from tqdm import tqdm
pd.options.mode.chained_assignment = None
from modeler.modeler import Modeler as m
from datetime import datetime, timedelta, timezone
import numpy as np
import math
import pickle
from sklearn.preprocessing import OneHotEncoder

class StockCategory(AProduct):
    def __init__(self,params):
        super().__init__("stock_category",
                        {"market":{}}
                        ,params)
    @classmethod
    def required_params(self):
        required = {}
        return required    
    
    def create_sim(self):
        self.db.connect()
        data = self.db.retrieve("sim")
        self.db.disconnect()
        sims = []
        if data.index.size > 0 :
            return data
        else:
            market = self.subscriptions["market"]["db"]
            market.connect()
            sp5 = market.retrieve("sp500")
            for ticker in tqdm(sp5["Symbol"].unique(),desc="stock_category sim"):
                try:
                    prices = market.retrieve_ticker_prices("prices",ticker)
                    prices = p.column_date_processing(prices)
                    prices["year"] = [x.year for x in prices["date"]]
                    prices["quarter"] = [x.quarter for x in prices["date"]]
                    quarterly_grouped = prices.groupby(["year","quarter","ticker"]).mean().reset_index()
                    categories = []
                    for value in quarterly_grouped["adjclose"]:
                        if value > 0 and value <= 100:
                            categories.append(100)
                        else:
                            if value > 100 and value <= 200:
                                categories.append(200)
                            else:
                                categories.append(500)
                    quarterly_grouped["category"] = categories
                    quarterly_grouped.reset_index(inplace=True)
                    groups = quarterly_grouped.merge(sp5.rename(columns={"Symbol":"ticker"}),on="ticker",how="left")
                    g = groups[["year","quarter","ticker","adjclose","category","GICS Sector"]]
                    g.dropna(inplace=True)
                    g["string_category"]  = [str(x) for x in g["category"]]
                    g["1_classification"] = "stock"
                    g["2_classification"] = g["GICS Sector"]
                    g["3_classification"] = g["string_category"] + g["2_classification"]
                    sim = g[["year","quarter","ticker","1_classification","2_classification","3_classification"]]
                    self.db.connect()
                    self.db.store("sim",sim)
                    self.db.disconnect()
                    sims.append(sim)
                except Exception as e:
                    print(str(e))
            return pd.concat(sims)