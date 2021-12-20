from strategy.rollingpercent import RollingPercent
from strategy.progressreport import ProgressReport
from strategy.financialpredict import FinancialPredict
from strategy.speculation import Speculation
from strategy.competition import Competition
class StratFact(object):
    @classmethod
    def create_strat(self,start,end,name,modeling_params={},trading_params={}):
        match name:
            case "rolling_percent":
                return RollingPercent(start,end,modeling_params,trading_params)
            case "progress_report":
                return ProgressReport(start,end,modeling_params,trading_params)
            case "financial_predict":
                return FinancialPredict(start,end,modeling_params,trading_params)
            case "speculation":
                return Speculation(start,end,modeling_params,trading_params)
            case "competition":
                return Competition(start,end,modeling_params,trading_params)
            case _:
                return None
