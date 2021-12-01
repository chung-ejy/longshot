from database.adatabase import ADatabase
import pandas as pd
class Portfolio(ADatabase):
    
    def __init__(self,name):
        super().__init__(name)
    