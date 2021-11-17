from database.market import Market
from database.sec import SEC

class DBFact(object):
    @classmethod
    def subscribe(self,dbtype):
        result = ""
        if dbtype == "market":
            result = Market()
        else:
            if dbtype == "sec":
                result = SEC()
            else:
                result = "whut"
        return result