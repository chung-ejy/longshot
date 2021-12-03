import pandas as pd
from strategy.astrategy import AStrategy
from datetime import timedelta, datetime
import pytz
from tqdm import tqdm
pd.options.mode.chained_assignment = None
from modeler.modeler import Modeler as m

class FinancialPredict(AStrategy):
    def __init__(self,start_date,end_date,params=
                        {"timeframe":"quarterly"
                    ,"score_requirement":70
                    ,"requirement":10
                    ,"category_training_year":4
                    ,"model_training_year":4
                    ,"value":True}):
        self.transformed=False
        super().__init__(f"financial_predict",
                            start_date,
                            end_date,
                        {"market":
                            {   "preload":True,
                                "tables":
                                { "prices":pd.DataFrame([{}]),
                                "sp500":pd.DataFrame([{}]),
                                "unified_financials":pd.DataFrame([{}])}
                            },
                        "stock_category":{
                            "preload":True,
                            "tables":{
                                "sim":pd.DataFrame([{}])
                            }
                        }},
                        params
                     )
    @classmethod
    def required_params(self):
        required =  {"timeframe":"quarterly"
                    ,"requirement":10
                    ,"value":True}
        return required

    def initial_transform(self):
        self.db.connect()
        data = self.db.retrieve("transformed")
        self.db.disconnect()
        financials = self.subscriptions["market"]["tables"]["unified_financials"]
        categories = self.subscriptions["stock_category"]["tables"]["sim"]
        prices = self.subscriptions["market"]["tables"]["prices"]
        sp5 = self.subscriptions["market"]["tables"]["sp500"]
        for i in range(40):
            financials.drop(str(i),inplace=True,axis=1,errors="ignore")
        cats = categories["prediction"].unique()
        categories["prediction"] = [x if x in cats else "None" for x in categories["prediction"]]
        categories["prediction"] = [x if x != None else "None" for x in categories["prediction"]]
        prices["date"] = pd.to_datetime(prices["date"])
        prices["year"] = [x.year for x in prices["date"]]
        prices["quarter"] = [x.quarter for x in prices["date"]]
        prices["week"] = [x.week for x in prices["date"]]
        financials["year"] = [row[1]["year"] + 1 if row[1]["quarter"] == 4 else row[1]["year"] for row in financials.iterrows()]
        financials["quarter"] = [1 if row[1]["quarter"] == 4 else row[1]["quarter"] + 1 for row in financials.iterrows()]
        financials = financials.groupby(["year","quarter","ticker"]).mean().reset_index()
        financials["date"] = [datetime(row[1]["year"], row[1]["quarter"] * 3 -  2, 1) for row in financials.iterrows()]
        valid = []
        for ticker in financials["ticker"].unique():
            if financials[financials["ticker"]==ticker].index.size == 49:
                valid.append(ticker)
        quarterly_sets = []
        start = self.start_date.year
        end = self.end_date.year
        if self.transformed or data.index.size > 1:
            self.transformed = True
            return data
        else:
            prices = prices[(prices["year"] >= start) & (prices["year"] <= end)]
            for ticker in tqdm(sp5["Symbol"].unique()):
                ticker_data = prices[prices["ticker"]==ticker]
                quarterly = ticker_data.groupby(["year","quarter"]).mean().reset_index()
                quarterly["y"] = quarterly["adjClose"].shift(-1)
                quarterly.dropna(inplace=True)
                quarterly["ticker"] = ticker
                quarterly_sets.append(quarterly)
            initial_data = pd.concat(quarterly_sets)
            initial_data["date"] = [datetime(row[1]["year"], row[1]["quarter"] * 3 -  2, 1) for row in initial_data.iterrows()]
            for param in self.params:
                initial_data[param] = self.params[param]
            self.db.connect()
            self.db.store("transformed",initial_data)
            self.db.disconnect()
            self.transformed = True
        return initial_data

    def create_sim(self):
        self.db.connect()
        sim = self.db.retrieve_sim(self.params)
        self.db.disconnect()
        if sim.index.size > 1:
            self.simmed = True
            return sim
        else:
            start_year = self.start_date.year
            end_year = self.end_date.year
            prices = self.subscriptions["market"]["tables"]["prices"].copy()
            financial_factors = ['AccumulatedOtherComprehensiveIncomeLossNetOfTax', 'Assets',
                                'AssetsCurrent', 'CashAndCashEquivalentsAtCarryingValue',
                                'EarningsPerShareBasic', 'EarningsPerShareDiluted',
                                'EntityCommonStockSharesOutstanding', 'IncomeTaxExpenseBenefit',
                                'LiabilitiesAndStockholdersEquity', 'LiabilitiesCurrent',
                                'NetIncomeLoss', 'OtherAssetsNoncurrent',
                                'RetainedEarningsAccumulatedDeficit', 'StockholdersEquity','CommonStockValue', 'Goodwill',
                                'PropertyPlantAndEquipmentNet']
            yearly_gap = 1
            analysis = []
            self.db.connect()
            initial_data = self.db.retrieve("transformed")
            self.db.disconnect()
            for col in self.params.keys():
                initial_data.drop(col,axis=1,inplace=True)
            financials = self.subscriptions["market"]["tables"]["unified_financials"]
            categories = self.subscriptions["stock_category"]["tables"]["sim"]
            category_training_year = self.params["category_training_year"]
            subset_categories = categories[categories["training_years"]==category_training_year]
            labels = initial_data.merge(subset_categories,on=["year","quarter","ticker"],how="left")
            factors = financials.merge(subset_categories,on=["year","quarter","ticker"],how="left")
            factors["date"] = [datetime(row[1]["year"], row[1]["quarter"] * 3 -  2, 1) for row in factors.iterrows()]
            model_training_year = self.params["model_training_year"]
            for year in range(start_year,end_year):
                for quarter in range(1,5):
                    quarterly_categories = labels[(labels["year"]==year) & (labels["quarter"]==quarter)]
                    for category in labels["prediction"].unique():
                        try:
                            category_tickers = labels[labels["prediction"]==category]["ticker"].unique()
                            training_factors = factors[factors["ticker"].isin(list(category_tickers))]
                            training_factors.sort_values("date",inplace=True)
                            if quarter == 1:
                                new_quarter = 4
                                new_year = year - 1
                            else:
                                new_quarter = quarter - 1
                                new_year = year
                            last_index = training_factors[(training_factors["year"] == new_year) & (training_factors["quarter"]==new_quarter)].index.values.tolist()[0]
                            spliced_training = training_factors.iloc[:last_index]
                            relevant_factors = spliced_training[(spliced_training["year"] < year) & (spliced_training["year"]>=year-model_training_year)]
                            training_labels = labels[labels["ticker"].isin(list(category_tickers))]
                            training_labels.sort_values("date",inplace=True)
                            last_index = training_labels[(training_labels["year"] == new_year) & (training_labels["quarter"]==new_quarter)].index.values.tolist()[0]
                            training_labels = training_labels.iloc[:last_index]
                            relevant_labels = training_labels[(training_labels["year"] < year) & (training_labels["year"]>=year-model_training_year)]
                            prediction_data = training_factors[(training_factors["year"]==year) & (training_factors["quarter"]==quarter)]
                            relevant = relevant_factors.merge(relevant_labels,on=["year","quarter","ticker"],how="left").dropna()
                            X = relevant[financial_factors]
                            y = relevant["adjClose"]
                            xgb_models = m.xgb_regression({"X":X,"y":y})
                            sk_models = m.sk_regression({"X":X,"y":y})
                            sk_models.append(xgb_models)
                            models = pd.DataFrame(sk_models)
                            model = models.sort_values("score",ascending=False).iloc[0]
                            sim = prediction_data
                            sim["quarterly_price_regression_prediction"] = model["model"].predict(sim[financial_factors])
                            sim["score"] = model["score"].item()
                            sim = sim[["year","quarter","ticker","quarterly_price_regression_prediction","score"]]
                            sim["model_training_year"] = model_training_year
                            sim["category_training_year"] = category_training_year
                            sim = prices.merge(sim,on=["year","quarter","ticker"],how="right").dropna()
                            sim["delta"] = (sim["quarterly_price_regression_prediction"] - sim["adjClose"]) / sim["adjClose"]
                            sim = sim[["year","quarter","ticker","adjClose","delta","score"]]
                            for param in self.params:
                                sim[param]=self.params[param]
                            if sim.index.size > 1:
                                self.db.connect()
                                self.db.store("sim",sim)
                                self.db.disconnect()
                        except Exception as e:
                            print(year,quarter,category,str(e)) 
                            continue
            self.simmed = True
        return relevant
               
    def daily_recommendation(self,date,sim,seat):
        if not self.params["value"]:
            sim["delta"] = sim["delta"] * -1
        while date.weekday() > 4:
            date = date + timedelta(days=1)
        try:
            daily_rec = sim[(sim["date"]>=date) & 
                        (sim["delta"] >= float(self.params["requirement"]/100))]
        except:
            daily_rec = sim[(sim["date"]>=date.astimezone(pytz.utc)) & 
                        (sim["delta"] >= float(self.params["requirement"]/100))]
        daily_rec = daily_rec[daily_rec["date"]==daily_rec["date"].min()].sort_values("delta",ascending=False)
        try:
            if daily_rec.index.size >= seat:
                result = daily_rec[["date","adjclose","ticker"]].iloc[seat].to_dict()
                result["seat"] = seat
                return result
            else:
                return {"error":"no trade","date":date}
        except Exception as e:
            return {"error":str(e)}
    
    def exit(self,sim,trade):
        bp = trade["adjclose"]
        sp = trade["adjclose"] * float(1+(self.params["requirement"]/100.0))
        try:
            this_exit = sim[(sim["date"] > trade["date"]) & (sim["adjclose"]>=sp)
            & (sim["ticker"]==trade["ticker"])].sort_values("date").iloc[0]
        except:
            this_exit = sim[(sim["date"] > trade["date"].astimezone(pytz.utc)) & (sim["adjclose"]>=sp)
            & (sim["ticker"]==trade["ticker"])].sort_values("date").iloc[0]
        trade["sell_date"] = this_exit["date"]
        trade["sell_price"] = sp
        return trade
        
