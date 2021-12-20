import pandas as pd
from strategy.astrategy import AStrategy
from processor.processor import Processor as p
from datetime import timedelta
import pytz
from tqdm import tqdm
from time import sleep
pd.options.mode.chained_assignment = None
class ProgressReport(AStrategy):
    def __init__(self,start_date,end_date,modeling_params={},trading_params={"value":True,"requirement":5}):
        super().__init__(f"progress_report",
                            start_date,
                            end_date,
                        {"market":{}},modeling_params=modeling_params,trading_params=trading_params)
        self.exit_days = 45
        self.last_call_day = 90
    @classmethod
    def required_params(self):
        required = {"timeframe":"quarterly"
                    ,"requirement":5}
        return required      

    def create_sim(self):
        if self.simmed:
            self.db.connect()
            sim = self.db.retrieve("sim")
            self.db.disconnect()
        else:
            start_year = self.start_date.year
            end_year = self.end_date.year
            market = self.subscriptions["market"]["db"]
            market.connect()
            self.db.connect()
            tickers = market.retrieve_tickers("prices")
            sim = []
            for ticker in tqdm(tickers["ticker"].unique(),desc=f"{self.name}_sim"):
                prices = market.retrieve_ticker_prices("prices",ticker)
                prices = p.column_date_processing(prices)
                prices["year"] = [x.year for x in prices["date"]]
                prices["quarter"] = [x.quarter for x in prices["date"]]
                for year in range(start_year,end_year):
                    for quarter in range(1,5):
                        try:
                            ticker_data = prices[(prices["year"]==year) & (prices["quarter"]==quarter)].sort_values("date")
                            sp = ticker_data.iloc[0]["adjclose"].item()
                            ticker_data["quarter_start"] = sp
                            ticker_data["delta"] = (ticker_data["adjclose"] - sp) / sp
                            ticker_data = ticker_data[["date","adjclose","delta","ticker"]]
                            for param in self.modeling_params:
                                ticker_data[param]=self.modeling_params[param]
                            sim.append(ticker_data)
                            self.db.store("sim",ticker_data)
                        except Exception as e:
                            continue
            sim = pd.concat(sim)
            self.db.disconnect()
            market.disconnect()
            self.simmed = True
        return sim
    
    def create_rec(self,date):
        self.db.connect()
        rec = self.db.query("rec",self.modeling_params)
        self.db.disconnect()
        if rec.index.size > 1:
            rec = p.column_date_processing(rec)
            small_rec = rec[rec["date"]>=date]
            if small_rec.index.size > 1:
                return small_rec
        else:
            year = date.year
            month = date.month
            quarter = int((month-1)/3) + 1
            market = self.subscriptions["market"]["db"]
            market.connect()
            self.db.connect()
            tickers = market.retrieve_tickers("prices")
            sim = []
            for ticker in tqdm(tickers["ticker"].unique(),desc=f"{self.name}_sim"):
                prices = market.retrieve_ticker_prices("prices",ticker)
                prices = p.column_date_processing(prices)
                prices["year"] = [x.year for x in prices["date"]]
                prices["quarter"] = [x.quarter for x in prices["date"]]
                try:
                    ticker_data = prices[(prices["year"]==year) & (prices["quarter"]==quarter)].sort_values("date")
                    sp = ticker_data.iloc[0]["adjclose"].item()
                    ticker_data["quarter_start"] = sp
                    ticker_data["delta"] = (ticker_data["adjclose"] - sp) / sp
                    ticker_data = ticker_data[["date","adjclose","delta","ticker"]]
                    for param in self.modeling_params:
                        ticker_data[param]=self.modeling_params[param]
                    sim.append(ticker_data.tail(1))
                    self.db.store("rec",ticker_data.tail(1))
                except Exception as e:
                    continue
            recs = pd.concat(sim)
            self.db.disconnect()
            market.disconnect()
            return recs