import pandas as pd
from strategy.astrategy import AStrategy
from processor.processor import Processor as p
from datetime import timedelta
import pytz
from tqdm import tqdm
from time import sleep
pd.options.mode.chained_assignment = None
class ProgressReport(AStrategy):
    def __init__(self,start_date,end_date,params={"timeframe":"quarterly"
                    ,"requirement":5}):
        super().__init__(f"progress_report",
                            start_date,
                            end_date,
                        {"market":{}
                            }
                            ,params)
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
            tickers = market.retrieve_tickers("prices")
            market.disconnect()
            sim = []
            for ticker in tqdm(tickers["ticker"].unique(),desc=f"{self.name}_sim"):
                market.connect()
                prices = market.retrieve_ticker_prices("prices",ticker)
                market.disconnect()
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
                            for param in self.params:
                                ticker_data[param]=self.params[param]
                            sim.append(ticker_data)
                            self.db.connect()
                            self.db.store("sim",ticker_data)
                            self.db.disconnect()
                        except Exception as e:
                            continue
            sim = pd.concat(sim)
            self.simmed = True
        return sim
               
    def daily_recommendation(self,date,sim,seat):
        while date.weekday() > 4:
            date = date + timedelta(days=1)
        try:
            daily_rec = sim[(sim["date"]>=date) & 
                        (sim["delta"] >= float(self.params["requirement"]/100))]
        except:
            daily_rec = sim[(sim["date"]>=date) & 
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
        this_exit = sim[(sim["date"] > trade["date"]) & (sim["adjclose"]>=sp)
         & (sim["ticker"]==trade["ticker"])].sort_values("date").iloc[0]
        trade["sell_date"] = this_exit["date"]
        trade["sell_price"] = sp
        return trade
        
