from database.market import Market
from database.sec import SEC
from database.product import Product
class DBFact(object):
    @classmethod
    def subscribe(self,dbtype):
        result = ""
        match dbtype:
            case "market":
                result = Market()
            case "sec":
                result = SEC()
            case "stock_category":
                result = Product("stock_category")
            case _:
                result = "whut"
        return result