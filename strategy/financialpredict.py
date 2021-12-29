import pandas as pd
from strategy.anaistrategy import AnAIStrategy
from datetime import timedelta, datetime
import pytz
from tqdm import tqdm
pd.options.mode.chained_assignment = None
from modeler.modeler import Modeler as m
from processor.processor import Processor as p
import numpy as np
class FinancialPredict(AnAIStrategy):
    def __init__(self,start_date,end_date,modeling_params={"score_requirement":70,"categories":2,"model_training_year":4},
                    trading_params= {"requirement":10,"value":True}):
        super().__init__("financial_predict",
                            start_date,
                            end_date,
                        {"market":{},
                        "stock_category":{}},modeling_params=modeling_params, trading_params=trading_params
                     )
        self.transformed = False
        self.exit_days = 45
        self.last_call_day = 90

    @classmethod
    def required_params(self):
        required =   {"modeling":{
                            "score_requirement":70
                            ,"model_training_year":4
                            ,"categories":2},
                    "trading": {
                    "requirement":10
                    ,"value":True }
                    }
        return required

    def initial_transform(self):
        self.db.connect()
        data = self.db.query("transformed",{})
        self.db.disconnect()
        if self.transformed or data.index.size > 1:
            self.transformed = True
            return data
        else:
            market = self.subscriptions["market"]["db"]
            quarterly_sets = []
            start = self.start_date.year
            end = self.end_date.year
            market.connect()
            sp5 = market.retrieve("sp500")
            self.db.connect()
            for ticker in tqdm(sp5["Symbol"].unique()):
                try:
                    prices = market.retrieve_ticker_prices("prices",ticker)
                    prices = p.column_date_processing(prices)
                    prices["year"] = [x.year for x in prices["date"]]
                    prices["quarter"] = [x.quarter for x in prices["date"]]
                    prices["week"] = [x.week for x in prices["date"]]
                    quarterly = prices.groupby(["year","quarter"]).mean().reset_index()
                    quarterly["y"] = quarterly["adjclose"].shift(-1)
                    quarterly.dropna(inplace=True)
                    quarterly["ticker"] = ticker
                    quarterly["date"] = [datetime(row[1]["year"], row[1]["quarter"] * 3 -  2, 1) for row in quarterly.iterrows()]
                    self.db.store("transformed",quarterly)          
                    quarterly_sets.append(quarterly)
                except Exception as e:
                    print(ticker,str(e))
                    continue
            initial_data = pd.concat(quarterly_sets)
            self.transformed = True
            market.disconnect()
            self.db.disconnect()
        return initial_data

    def create_sim(self):
        self.db.connect()
        sim = self.db.retrieve_sim(self.modeling_params)
        self.db.disconnect()
        if sim.index.size > 1:
            self.simmed = True
            return sim
        else:
            market = self.subscriptions["market"]["db"]
            product_db = self.subscriptions["stock_category"]["db"]
            categories_nums = self.modeling_params["categories"]
            market.connect()
            product_db.connect()
            financials = market.retrieve("unified_financials")
            sp5 = market.retrieve("sp500")
            categories = product_db.retrieve("sim")
            product_db.disconnect()
            financials["year"] = [x.year for x in financials["date"]]
            financials["quarter"] = [x.quarter for x in financials["date"]]
            financials["year"] = [row[1]["year"] + 1 if row[1]["quarter"] == 4 else row[1]["year"] for row in financials.iterrows()]
            financials["quarter"] = [1 if row[1]["quarter"] == 4 else row[1]["quarter"] + 1 for row in financials.iterrows()]
            financials = financials.groupby(["year","quarter","ticker"]).mean().reset_index()
            financials["date"] = [datetime(row[1]["year"], row[1]["quarter"] * 3 -  2, 1) for row in financials.iterrows()]
            start = self.start_date.year
            end = self.end_date.year
            financial_factors = [x for x in financials.columns if x not in ["year","quarter","ticker","date"]]
            yearly_gap = 1
            self.db.connect()
            initial_data = self.db.retrieve("transformed")
            labels = initial_data.merge(categories,on=["year","quarter","ticker"],how="left")
            factors = financials.merge(categories,on=["year","quarter","ticker"],how="left")
            factors["date"] = [datetime(row[1]["year"], row[1]["quarter"] * 3 -  2, 1) for row in factors.iterrows()]
            model_training_year = self.modeling_params["model_training_year"]
            start_year = self.start_date.year
            end_year = self.end_date.year
            sims = []
            for year in tqdm(range(start_year,end_year+1),desc="financial_predict_sim_year"):
                for quarter in range(1,5):
                    quarterly_categories = labels[(labels["year"]==year) & (labels["quarter"]==quarter)]
                    for category in list(labels[f"{categories_nums}_classification"].unique()):
                        try:
                            category_tickers = labels[labels[f"{categories_nums}_classification"]==category]["ticker"].unique()
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
                            relevant.reset_index(inplace=True)
                            X = relevant[financial_factors]
                            y = relevant["adjclose"]
                            models = m.regression({"X":X,"y":y})
                            models["year"] = year
                            models["quarter"] = quarter
                            models["category"] = category
                            sim = prediction_data
                            sim = sim.groupby(["year","quarter","ticker"]).mean().reset_index()
                            for i in range(models.index.size):
                                model = models.iloc[i]
                                api = model["api"]
                                score = model["score"]
                                if score >= self.modeling_params["score_requirement"]/100:
                                    sim[f"{api}_prediction"] = model["model"].predict(sim[financial_factors])
                                    sim[f"{api}_score"] = model["score"].item()
                            stuff = []
                            for ticker in category_tickers:
                                ticker_data = market.retrieve_ticker_prices("prices",ticker)
                                stuff.append(ticker_data)
                            prices = p.column_date_processing(pd.concat(stuff))
                            prices["year"] = [x.year for x in prices["date"]]
                            prices["quarter"] = [x.quarter for x in prices["date"]]
                            sim = prices.merge(sim,on=["year","quarter","ticker"],how="left").dropna()
                            sim["categories"] = categories_nums
                            final_cols = ["date","ticker","adjclose","categories"]
                            final_cols.extend([x for x in list(sim.columns) if "prediction" in x or "score" in x])
                            sim = sim[final_cols]
                            sim.fillna(0,inplace=True)
                            sim["prediction"] = [np.nanmean([row[1][x] for x in sim.columns if "prediction" in x and row[1][x] != 0]) for row in sim.iterrows()]
                            sim["score"] = [np.nanmean([row[1][x] for x in sim.columns if "score" in x and row[1][x] != 0]) for row in sim.iterrows()]
                            sim["delta"] = (sim["prediction"] - sim["adjclose"]) / sim["adjclose"]
                            for param in self.modeling_params:
                                sim[param]=self.modeling_params[param]
                            if sim.index.size > 1:
                                self.db.store("sim",sim)
                                sims.append(sim)
                        except Exception as e:
                            print(year,quarter,category,str(e)) 
                            continue
            self.simmed = True
            self.db.disconnect()
            market.disconnect()
            return pd.concat(sims)