from strategy.rollingpercent import RollingPercent
from strategy.progressreport import ProgressReport
class StratFact(object):
    @classmethod
    def create_strat(self,start,end,name,params):
        match name:
            case "rolling_percent":
                return RollingPercent(start,end,params)
            case "progress_report":
                return ProgressReport(start,end,params)
            case _:
                return None
