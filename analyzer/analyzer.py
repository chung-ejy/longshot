
from datetime import timedelta,datetime
import pandas as pd
from database.market import Market
class Analyzer(object):
    @classmethod
    def pv_analysis(self,portfolio):
        stuff = []
        total_cash = 100
        trades = portfolio.trades
        trades = trades[(trades["date"] >= portfolio.start) & (trades["date"] <= portfolio.end)]
        trades["quarter"] = [x.quarter for x in trades["sell_date"]]
        trades["year"] = [x.year for x in trades["sell_date"]]
        if trades.index.size < 1:
            return pd.DataFrame([{"message":"no trades..."}])
        number_of_strats = len(portfolio.strats.keys())
        for strategy in list(portfolio.strats.keys()):
            strat_trades = trades[trades["strategy"]==strategy]
            cash = []
            for seat in range(portfolio.seats):
                initial = float(total_cash / number_of_strats / portfolio.seats )
                seat_trades = strat_trades[strat_trades["seat"]==seat]
                for delta in seat_trades["delta"]:
                    initial = initial * (1+delta)
                    cash.append(initial)
            strat_trades["pv"] = cash
            stuff.append(strat_trades)
        analysis = pd.concat(stuff).pivot_table(index=["strategy","date"],columns="seat",values="pv").fillna(method="ffill").fillna(float(total_cash / number_of_strats / portfolio.seats )).reset_index()
        analysis["pv"] = [sum([row[1][i] for i in range(portfolio.seats)]) for row in analysis.iterrows()]
        final = analysis.pivot_table(index="date",columns="strategy",values="pv").fillna(method="ffill").fillna(float(total_cash / number_of_strats)).reset_index()
        return final
    
    @classmethod
    def industry_analysis(self,portfolio):
        market = Market()
        market.connect()
        sp5 = market.retrieve("sp500")
        market.disconnect()
        sp5.rename(columns={"Symbol":"ticker"},inplace=True)
        trades = portfolio.trades
        trades["quarter"] = [x.quarter for x in trades["sell_date"]]
        trades["year"] = [x.year for x in trades["sell_date"]]
        if trades.index.size < 1:
            return pd.DataFrame([{"message":"no trades..."}])
        trades = trades.merge(sp5,on="ticker",how="left")
        # industry check
        final = trades.groupby(["year","quarter","strategy","GICS Sector"]).mean().sort_values("delta",ascending=False).reset_index()[["year","quarter","strategy","GICS Sector","delta"]].head(10)
        return final