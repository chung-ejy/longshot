
from datetime import timedelta,datetime
import pandas as pd
class Backtester(object):
    @classmethod
    def equity_timeseries_backtest(self,start_date,end_date,seats,strat):
        trades = []
        seats
        sim = strat.create_sim()
        try:
            sim["date"] = pd.to_datetime(sim["date"])
        except Exception as e:
            print(str(e))
        params = strat.params
        blacklist = []
        for seat in range(seats):
            date = start_date
            while date < end_date:
                try:
                    if date.weekday() > 4:
                        date = date + timedelta(days=1)
                    else:
                        if len(blacklist) > 0:
                            bl = pd.DataFrame(blacklist)
                            bl_tickers = bl[(bl["date"] <= date) & (bl["sell_date"]>= date)]["ticker"].unique()
                            if len(bl_tickers) > 0:
                                filtered_sim = sim[~sim["ticker"].isin(bl_tickers)]
                            else:
                                filtered_sim = sim
                        else:
                            filtered_sim = sim
                        daily_rec = strat.daily_recommendation(date,filtered_sim,seat)
                        if "error" in daily_rec.keys():
                            date = date + timedelta(days=1)
                        else:
                            trade = strat.exit(sim,daily_rec)
                            trade = {**trade,**params}
                            trades.append(trade)
                            blacklist.append(trade)
                            date = trade["sell_date"] + timedelta(days=1)
                except Exception as e:
                    print(str(e))
                    date = date + timedelta(days=1)
        t = pd.DataFrame(trades)
        return t