from strategy.financialpredict import FinancialPredict
from datetime import datetime, timedelta
start_date = datetime(2015,1,1)
end_date = datetime(2021,1,1)
fp = FinancialPredict(start_date,end_date)
print(fp.params)