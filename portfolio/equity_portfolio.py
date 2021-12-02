import pandas as pd
from portfolio.aportfolio import APortfolio
from strategy.stratfact import StratFact
from datetime import timedelta
import pytz
from tqdm import tqdm
pd.options.mode.chained_assignment = None
from datetime import datetime, timedelta, timezone
from backtester.backtester import Backtester
class EquityPortfolio(APortfolio):
    def __init__(self,start,end,name,weighting="equal",seats=5,strats={
                    "rolling_percent":{"params":{"timeframe":"daily"
                                                ,"requirement":5
                                                ,"days":7
                                                ,"value":True
                                                ,"currency":"prices"}
                                        },
                    "progress_report":{"params":{"timeframe":"quarterly"
                                                ,"requirement":5}
                                    }
                    }):
        self.start = start
        self.end = end
        self.strats = strats
        self.seats = seats
        self.weighting = weighting
        super().__init__(name)
    
    @classmethod
    def required_params(self):
        required = {"rolling_percent":{"params":{"timeframe":"daily"
                ,"requirement":5
                ,"days":7
                ,"value":True
                ,"currency":"prices"}},
                            "progress_report":{"params":{"timeframe":"quarterly"
                    ,"requirement":5}}}
        return required    
    
    def load(self):
        for strat in tqdm(self.strats):
            strat_params = self.strats[strat]["params"]
            strat_class = StratFact.create_strat(self.start,self.end,strat,strat_params)
            strat_class.subscribe()
            strat_class.load()
            self.strats[strat]["class"] = strat_class

    
    def sim(self):
        for strat in tqdm(self.strats):
            strat_class = self.strats[strat]["class"]
            strat_class.create_sim()
    
    def backtest(self):
        trades = []
        for strat in tqdm(self.strats.keys()):
            try:
                self.db.connect()
                query = self.strats[strat]["params"]
                query["strategy"] = strat
                t = self.db.retrieve_trades(query)
                self.db.disconnect()
                if t.index.size < 1:
                    strat_class = self.strats[strat]["class"]
                    t = Backtester.equity_timeseries_backtest(self.start,self.end,self.seats,strat_class)
                    t["strategy"] = strat
                    t["delta"] = (t["sell_price"] - t["adjclose"]) / t["adjclose"]
                    self.db.connect()
                    self.db.store("trades",t)
                    self.db.disconnect()
                trades.append(t)
            except Exception as e:
                print(str(e))
                continue   
        self.trades = pd.concat(trades)
        return self.trades
    