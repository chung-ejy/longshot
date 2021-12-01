
from datetime import timedelta,datetime
import pandas as pd
class Backtester(object):
    @classmethod
    def equity_timeseries_backtest(self,start_date,end_date,seats,strat):
        trades = []
        seats
        sim = strat.create_sim()
        params = strat.params
        for seat in range(seats):
            date = start_date
            while date < end_date:
                try:
                    if date.weekday() > 4:
                        date = date + timedelta(days=1)
                    else:
                        daily_rec = strat.daily_recommendation(date,sim,seat)
                        if "error" in daily_rec.keys():
                            date = date + timedelta(days=1)
                        else:
                            trade = strat.exit(sim,daily_rec)
                            trade = {**trade,**params}
                            trades.append(trade)
                            date = trade["sell_date"] + timedelta(days=1)
                except Exception as e:
                    print(str(e))
                    date = date + timedelta(days=1)
        t = pd.DataFrame(trades)
        return t