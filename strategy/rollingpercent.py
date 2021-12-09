import pandas as pd
from strategy.astrategy import AStrategy
from datetime import timedelta
import pytz
from tqdm import tqdm
pd.options.mode.chained_assignment = None
from processor.processor import Processor as p
class RollingPercent(AStrategy):
    def __init__(self,start_date,end_date,params=
                        {"timeframe":"daily"
                    ,"requirement":5
                    ,"days":7
                    ,"value":True
                    ,"currency":"crypto"}):
        super().__init__(f"rolling_percent",
                            start_date,
                            end_date,
                        {"market":
                                {"tables":[params["currency"]]}
                            },
                        params
                     )
    @classmethod
    def required_params(self):
        required = {"timeframe":"daily"
                    ,"requirement":5
                    ,"days":7
                    ,"value":True
                    ,"currency":"crypto"}
        return required

    def create_sim(self):
        self.db.connect()
        sim = self.db.retrieve_sim(self.params)
        self.db.disconnect()
        if sim.index.size > 1:
            self.simmed = True
            return sim
        else:
            start_year = self.start_date.year
            end_year = self.end_date.year
            sim = []
            days = self.params["days"]
            market = self.subscriptions["market"]["db"]
            market.connect()
            tickers = market.retrieve_tickers(self.params["currency"])
            market.disconnect()
            for ticker in tqdm(tickers["ticker"].unique(),desc=f'{self.name}_sim'):
                market.connect()
                prices = market.retrieve_ticker_prices(self.params["currency"],ticker)
                market.disconnect()
                prices = p.column_date_processing(prices)
                prices["year"] = [x.year for x in prices["date"]]
                prices["quarter"] = [x.quarter for x in prices["date"]]
                prices.sort_values("date",inplace=True)
                prices[f'rolling_{days}'] = prices["adjclose"].rolling(window=days).mean()
                prices = prices[(prices["year"] >= start_year) & (prices["year"]<=end_year)].dropna()
                prices["delta"] = (prices[f"rolling_{days}"] - prices["adjclose"]) / prices[f"rolling_{days}"]
                prices = prices[["date","adjclose","delta","ticker",f"rolling_{days}"]]
                for param in self.params:
                    prices[param]=self.params[param]
                self.db.connect()
                self.db.store("sim",prices)
                self.db.disconnect()
                sim.append(prices)
            sim = pd.concat(sim)
            self.simmed = True
        return sim
               
    def daily_recommendation(self,date,sim,seat):
        if not self.params["value"]:
            sim["delta"] = sim["delta"] * -1
        while date.weekday() > 4:
            date = date + timedelta(days=1)
        try:
            daily_rec = sim[(sim["date"]>=date) & 
                        (sim["delta"] >= float(self.params["requirement"]/100))]
        except:
            daily_rec = sim[(sim["date"]>=date.astimezone(pytz.utc)) & 
                        (sim["delta"] >= float(self.params["requirement"]/100))]
        daily_rec = daily_rec[daily_rec["date"]==daily_rec["date"].min()].sort_values("delta",ascending=False)
        try:
            if daily_rec.index.size >= seat:
                result = daily_rec[["date","adjclose","ticker"]].iloc[seat].to_dict()
                result["seat"] = seat
                return result
            else:
                return {"error":"no trade","date":date}
        except Exception as e:
            return {"error":str(e)}
    
    def exit(self,sim,trade):
        bp = trade["adjclose"]
        sp = trade["adjclose"] * float(1+(self.params["requirement"]/100.0))
        try:
            this_exit = sim[(sim["date"] > trade["date"]) & (sim["adjclose"]>=sp)
            & (sim["ticker"]==trade["ticker"])].sort_values("date").iloc[0]
        except:
            this_exit = sim[(sim["date"] > trade["date"].astimezone(pytz.utc)) & (sim["adjclose"]>=sp)
            & (sim["ticker"]==trade["ticker"])].sort_values("date").iloc[0]
        trade["sell_date"] = this_exit["date"]
        trade["sell_price"] = sp
        return trade
        
