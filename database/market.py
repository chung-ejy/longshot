from database.adatabase import ADatabase

class Market(ADatabase):
    
    def __init__(self):
        super().__init__("project_market")