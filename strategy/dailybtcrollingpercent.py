import pandas as pd
from strategy.astrategy import AStrategy

class DailyBTCRollingPercent(AStrategy):
    def __init__(self):
        super().__init__("daily_btc_rolling_percent"
                        ,{"market":{"table":"crypto"
                                    }}
                        ,"daily")
    
    # def create_training_set(self):
    #     training_days = 100
        
