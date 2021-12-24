import pandas as pd
from strategy.astrategy import AStrategy
from datetime import timedelta
import pytz
from tqdm import tqdm
pd.options.mode.chained_assignment = None
from processor.processor import Processor as p
class RollingPercent(AStrategy):
    def __init__(self,start_date,end_date,modeling_params={"days":7,"currency":"crypto"},
                        trading_params={"requirement":5,"value":True}):
        super().__init__(f"rolling_percent",
                            start_date,
                            end_date,
                        {"market":{}},modeling_params=modeling_params, trading_params=trading_params
                     )
        self.exit_days = 45
        self.last_call_day = 90
    @classmethod
    def required_params(self):
        required = {"modeling":{"days":7,"currency":"crypto"},
                        "trading":{"requirement":5,"value":True}}
        return required

    def create_sim(self):
        self.db.connect()
        sim = self.db.retrieve_sim(self.modeling_params)
        self.db.disconnect()
        if sim.index.size > 1:
            self.simmed = True
            return sim
        else:
            start_year = self.start_date.year
            end_year = self.end_date.year
            sim = []
            days = self.modeling_params["days"]
            market = self.subscriptions["market"]["db"]
            market.connect()
            tickers = market.retrieve_tickers(self.modeling_params["currency"])
            market.disconnect()
            for ticker in tqdm(tickers["ticker"].unique(),desc=f'{self.name}_sim'):
                market.connect()
                prices = market.retrieve_ticker_prices(self.modeling_params["currency"],ticker)
                market.disconnect()
                prices = p.column_date_processing(prices)
                prices["year"] = [x.year for x in prices["date"]]
                prices["quarter"] = [x.quarter for x in prices["date"]]
                prices.sort_values("date",inplace=True)
                prices[f'rolling_{days}'] = prices["adjclose"].rolling(window=days).mean()
                prices = prices[(prices["year"] >= start_year) & (prices["year"]<=end_year)].dropna()
                prices["delta"] = (prices[f"rolling_{days}"] - prices["adjclose"]) / prices[f"rolling_{days}"]
                prices = prices[["date","adjclose","delta","ticker",f"rolling_{days}"]]
                for param in self.modeling_params:
                    prices[param]=self.modeling_params[param]
                self.db.connect()
                self.db.store("sim",prices)
                self.db.disconnect()
                sim.append(prices)
            sim = pd.concat(sim)
            self.simmed = True
        return sim
    
    def create_rec(self,date):
        self.db.connect()
        rec = self.db.query("rec",self.modeling_params)
        self.db.disconnect()
        rec = p.column_date_processing(rec)
        if rec.index.size > 1:
            rec = p.column_date_processing(rec)
            small_rec = rec[rec["date"]>=date]
            if small_rec.index.size > 1:
                return small_rec
        else:
            year = date.year
            month = date.month
            quarter = int((month-1)/3) + 1
            sim = []
            days = self.modeling_params["days"]
            market = self.subscriptions["market"]["db"]
            market.connect()
            tickers = market.retrieve_tickers(self.modeling_params["currency"])
            market.disconnect()
            for ticker in tqdm(tickers["ticker"].unique(),desc=f'{self.name}_sim'):
                market.connect()
                prices = market.retrieve_ticker_prices(self.modeling_params["currency"],ticker)
                market.disconnect()
                prices = p.column_date_processing(prices)
                prices["year"] = [x.year for x in prices["date"]]
                prices["quarter"] = [x.quarter for x in prices["date"]]
                prices.sort_values("date",inplace=True)
                prices[f'rolling_{days}'] = prices["adjclose"].rolling(window=days).mean()
                prices = prices[(prices["year"] >= year - 1)].dropna()
                prices["delta"] = (prices[f"rolling_{days}"] - prices["adjclose"]) / prices["adjclose"]
                prices = prices[["date","adjclose","delta","ticker",f"rolling_{days}"]]
                for param in self.modeling_params:
                    prices[param]=self.modeling_params[param]
                self.db.connect()
                self.db.store("rec",prices.tail(1))
                self.db.disconnect()
                sim.append(prices.tail(1))
            sim = pd.concat(sim)
            self.simmed = True
            return sim
