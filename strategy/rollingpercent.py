import pandas as pd
from strategy.astrategy import AStrategy
from datetime import timedelta
import pytz
from tqdm import tqdm
pd.options.mode.chained_assignment = None
class RollingPercent(AStrategy):
    def __init__(self,start_date,end_date,params):
        super().__init__(f"rolling_percent",
                            start_date,
                            end_date,
                        {"market":
                            {   "preload":True,
                                "tables":
                                { params["currency"]:pd.DataFrame([{}])}
                            }
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
        if self.simmed:
            self.db.connect()
            sim = self.db.retrieve_sim(self.params)
            self.db.disconnect()
        else:
            start_year = self.start_date.year
            end_year = self.end_date.year
            prices = self.subscriptions["market"]["tables"][self.params["currency"]].copy()
            new_cols = {}
            for column in prices.columns:
                new_cols[column] = column.lower().replace(" ","")
            for col in new_cols:
                prices.rename(columns={col:new_cols[col]},inplace=True)
            try:
                prices["date"] = pd.to_datetime(prices["Date"])
            except:
                prices["date"] = pd.to_datetime(prices["date"])
            prices["year"] = [x.year for x in prices["date"]]
            prices["quarter"] = [x.quarter for x in prices["date"]]
            sim = []
            days = self.params["days"]
            for ticker in tqdm(prices["ticker"].unique()):
                ticker_data = prices[prices["ticker"]==ticker]
                relevant = ticker_data
                relevant.sort_values("date",inplace=True)
                relevant[f'rolling_{days}'] = relevant["adjclose"].rolling(window=days).mean()
                relevant = relevant[(relevant["year"] >= start_year) & (relevant["year"]<=end_year)].dropna()
                relevant["delta"] = (relevant[f"rolling_{days}"] - relevant["adjclose"]) / relevant[f"rolling_{days}"]
                relevant = relevant[["date","adjclose","delta","ticker",f"rolling_{days}"]]
                for param in self.params:
                    relevant[param]=self.params[param]
                self.db.connect()
                self.db.store("sim",relevant)
                self.db.disconnect()
                sim.append(relevant)
            sim = pd.concat(sim)
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
        
