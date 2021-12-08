from strategy.rollingpercent import RollingPercent
from strategy.progressreport import ProgressReport
from strategy.financialpredict import FinancialPredict
from strategy.speculation import Speculation
class StratFact(object):
    @classmethod
    def create_strat(self,start,end,name,params):
        match name:
            case "rolling_percent":
                return RollingPercent(start,end,params)
            case "progress_report":
                return ProgressReport(start,end,params)
            case "financial_predict":
                return FinancialPredict(start,end)
            case "speculation":
                return Speculation(start,end)
            case _:
                return None
