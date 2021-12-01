import pandas as pd
from portfolio.aportfolio import APortfolio
from strategy.stratfact import StratFact
from datetime import timedelta
import pytz
from tqdm import tqdm
pd.options.mode.chained_assignment = None
from modeler.modeler import Modeler as m
from datetime import datetime, timedelta, timezone
import numpy as np
import math
import pickle
from sklearn.preprocessing import OneHotEncoder
from tqdm import tqdm
class EquityPortfolio(APortfolio):
    def __init__(self,start,end,name,params={"strats":{"rolling_percent":{"params":{"timeframe":"daily"
                ,"requirement":5
                ,"days":7
                ,"value":True
                ,"currency":"prices"}},
                            "progress_report":{"params":{"timeframe":"quarterly"
                    ,"requirement":5}}},
                    "weighting":"equal"}):
        self.start = start
        self.end = end
        self.params = params
        super().__init__(name)
    
    @classmethod
    def required_params(self):
        required = {"strats":{"rolling_percent":{"params":{"timeframe":"daily"
                ,"requirement":5
                ,"days":7
                ,"value":True
                ,"currency":"prices"}},
                            "progress_report":{"params":{"timeframe":"quarterly"
                    ,"requirement":5}}},
                    "weighting":"equal"}
        return required    
    
    def load(self):
        for strat in tqdm(self.params["strats"]):
            strat_params = self.params["strats"][strat]["params"]
            strat_class = StratFact.create_strat(self.start,self.end,strat,strat_params)
            strat_class.subscribe()
            strat_class.load()
            self.params["strats"][strat]["class"] = strat_class

    
    def sim(self):
        for strat in tqdm(self.params["strats"]):
            strat_class = self.params["strats"][strat]["class"]
            strat_class.create_sim()
    
    def backtest(self):
        for strat in tqdm(self.params["strats"]):
            strat_class = self.params["strats"][strat]["class"]
            strat_class.backtest()
    