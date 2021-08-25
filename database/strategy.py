from database.adatabase import ADatabase
import pandas as pd
class Strategy(ADatabase):
    
    def __init__(self):
        super().__init__("project_strategy")
