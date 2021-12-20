
from datetime import timedelta,datetime
from processor.processor import Processor as p
import pandas as pd
from tqdm import tqdm
class Backtester(object):
    def __init__(self,strat):
        self.strat = strat
    
    def equity_timeseries_backtest(self,start_date,end_date,seats):
        trades = []
        sim = self.strat.create_sim()
        sim = p.column_date_processing(sim)
        params = self.strat.trading_params
        blacklist = []
        phase = "base"
        if not params["value"]:
            sim["delta"] = sim["delta"] * -1
        for seat in tqdm(range(seats),desc="backtesting_seats"):
            date = start_date
            while date < end_date:
                try:
                    if date.weekday() > 4:
                        date = date + timedelta(days=1)
                    else:
                        phase = "blacklist"
                        if len(blacklist) > 0:
                            bl = pd.DataFrame(blacklist)
                            bl_tickers = bl[(bl["date"] <= date) & (bl["sell_date"]>= date)]["ticker"].unique()
                            if len(bl_tickers) > 0:
                                filtered_sim = sim[~sim["ticker"].isin(bl_tickers)]
                            else:
                                filtered_sim = sim
                        else:
                            filtered_sim = sim
                        if filtered_sim.index.size > 1:
                            phase = "daily_recs"
                            daily_rec = self.base_daily_rec(date,filtered_sim,seat)
                            if "error" in daily_rec.keys():
                                date = date + timedelta(days=1)
                            else:
                                phase = "exit"
                                trade = self.base_exit(sim,daily_rec)
                                if trade["sell_date"] < trade["date"]:
                                    date = date + timedelta(days=1)
                                else:
                                    trades.append(trade)
                                    blacklist.append(trade)
                                    date = trade["sell_date"] + timedelta(days=1)
                        else:
                            date = date + timedelta(days=1)
                except Exception as e:
                    print(phase,str(e))
                    date = date + timedelta(days=1)
        t = pd.DataFrame(trades)
        return t
        
    def base_daily_rec(self,date,sim,seat):
        daily_rec = sim[(sim["date"]>=date) 
                & (sim["delta"] >= float(self.strat.trading_params["requirement"]/100))]
        daily_rec = daily_rec[daily_rec["date"]==daily_rec["date"].min()].sort_values("delta",ascending=False)
        try:
            if daily_rec.index.size >= seat:
                result = daily_rec[["date","adjclose","ticker","delta"]].iloc[seat].to_dict()
                result["seat"] = seat
                return result
            else:
                return {"error":"no trade","date":date}
        except Exception as e:
            return {"error":str(e)}
    
    def base_exit(self,sim,trade):
        bp = trade["adjclose"]
        if self.strat.ai:
            sp = trade["adjclose"] * min(2,float(1+trade["delta"]))
        else:
            sp = trade["adjclose"] * float(1+(self.strat.trading_params["requirement"]/100.0))
        min_date = trade["date"] + timedelta(days=1)
        exit_date  = trade["date"] + timedelta(days=self.strat.exit_days)
        phase = "exiting"
        tsim = sim[sim["ticker"]==trade["ticker"]].copy()
        last_call_days = min((tsim["date"].max() - trade["date"]).days-1,self.strat.last_call_day)
        cover_date = trade["date"] + timedelta(days=last_call_days)
        best_exits = tsim[(tsim["date"] >= min_date) & (tsim["date"] <= exit_date) & (tsim["adjclose"]>=sp)].sort_values("date",ascending=True).copy()
        breakeven_exits = tsim[(tsim["date"] > exit_date) & (tsim["date"] <= cover_date) & (tsim["adjclose"] >= bp)].sort_values("date",ascending=True).copy()
        rekt_exits = tsim[(tsim["date"] > cover_date)].sort_values("date",ascending=True).copy()
        if best_exits.index.size < 1:
            if breakeven_exits.index.size < 1:
                if rekt_exits.index.size < 1:
                    the_exit = trade
                    trade["sell_price"] = trade["adjclose"]
                else:
                    the_exit = rekt_exits.iloc[0]
                    trade["sell_price"] = the_exit["adjclose"]
            else:
                the_exit = breakeven_exits.iloc[0]
                trade["sell_price"] = bp
        else:
            the_exit = best_exits.iloc[0]
            trade["sell_price"] = sp
        trade["sell_date"] = the_exit["date"]
        trade = {**trade,**self.strat.trading_params}
        return trade