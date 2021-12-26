import pandas as pd
from portfolio.aportfolio import APortfolio
from strategy.stratfact import StratFact
from datetime import timedelta
import pytz
from tqdm import tqdm
pd.options.mode.chained_assignment = None
from datetime import datetime, timedelta, timezone
from backtester.backtester import Backtester
from tqdm import tqdm
class EquityPortfolio(APortfolio):
    def __init__(self,start,end,name,weighting="equal",seats=10,strats={}):
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
            modeling_params = self.strats[strat]["modeling_params"]
            trading_params = self.strats[strat]["trading_params"]
            strat_class = StratFact.create_strat(self.start,self.end,strat,modeling_params,trading_params)
            strat_class.subscribe()
            self.strats[strat]["class"] = strat_class
        
    def transform(self):
        for strat in tqdm(self.strats):
            strat_class = self.strats[strat]["class"]
            if strat_class.ai:
                strat_class.initial_transform()
            else:
                continue
        
    def sim(self):
        for strat in tqdm(self.strats):
            strat_class = self.strats[strat]["class"]
            strat_class.create_sim()
    
    def backtest(self):
        trades = []
        for strat in tqdm(self.strats.keys()):
            try:
                self.db.connect()
                query = {**self.strats[strat]["class"].trading_params}
                query["strategy"] = strat
                t = self.db.retrieve_trades(query)
                self.db.disconnect()
                if t.index.size < 1:
                    strat_class = self.strats[strat]["class"]
                    b = Backtester(strat_class)
                    t = b.equity_timeseries_backtest(self.start,self.end,self.seats)
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
    