import pandas as pd
import database.competition as dbc
from strategy.anaistrategy import AnAIStrategy
from datetime import timedelta, datetime
import pytz
from tqdm import tqdm
pd.options.mode.chained_assignment = None
from modeler.modeler import Modeler as m
from processor.processor import Processor as p
import numpy as np
import pickle
class Competition(AnAIStrategy):
    def __init__(self,start_date,end_date,modeling_params={
                    "categories":2
                    ,"model_training_year":1
                    ,"score_requirement":70
                        },
                    trading_params={
                    "score_requirement":70
                    ,"requirement":5
                    ,"value":True}):
        super().__init__("competition",
                            start_date,
                            end_date,
                        {"market":{},
                        "stock_category":{}},modeling_params=modeling_params, trading_params=trading_params
                     )
        self.transformed = False
        self.db = dbc.Competition("competition")
        self.exit_days = 14
        self.last_call_day = 28

    @classmethod
    def required_params(self):
        required =  {
                    "requirement":5
                    ,"score_requirement":70
                    ,"number_of_training_weeks":14
                    ,"value":True}
        return required

    def initial_transform(self):
        self.db.connect()
        data = self.db.transform_check({"categories":self.modeling_params["categories"]})
        market = self.subscriptions["market"]["db"]
        stock_category = self.subscriptions["stock_category"]["db"]
        if self.transformed or data.index.size > 1:
            self.transformed = True
            return data
        else:
            phase = "initial loads"
            self.db.connect()
            market = self.subscriptions["market"]["db"]
            stock_category = self.subscriptions["stock_category"]["db"]
            market.connect()
            stock_category.connect()
            categories = stock_category.retrieve("sim")
            sp5 = market.retrieve("sp500")
            weekly_gap = 1
            weekly_sets = []
            num_cats = self.modeling_params["categories"]
            categories = categories[categories["year"] >= self.start_date.year - self.modeling_params["model_training_year"]]
            for category in tqdm(categories[f"{num_cats}_classification"].unique()):
                try:
                    cols = categories[categories[f"{num_cats}_classification"]==category]["ticker"].unique()
                    stuff = []
                    phase = "first concat"
                    for t in cols:
                        ticker_data = market.retrieve_ticker_prices("prices",t)
                        ticker_data = p.column_date_processing(ticker_data)
                        stuff.append(ticker_data)
                    heh = pd.concat(stuff)
                    lel = heh.pivot_table(index="date",columns="ticker",values="adjclose").reset_index()
                    lel["year"] = [x.year for x in lel["date"]]
                    lel["week"] = [x.week for x in lel["date"]]
                    lel["quarter"] = [x.quarter for x in lel["date"]]
                    weekly = lel.groupby(["year","quarter","week"]).mean().reset_index()
                    weekly = weekly[weekly["year"] >= self.start_date.year - self.modeling_params["model_training_year"]]
                    for ticker in cols:
                        ticker_weekly = weekly.copy()
                        ticker_weekly["y"] = ticker_weekly[ticker].shift(-weekly_gap)
                        ticker_weekly = ticker_weekly[:-1].copy()
                        relevant = ["year","quarter","week","y"]
                        phase = "finding relevant columns"
                        for col in cols:
                            if 0.0 not in list(ticker_weekly[col].fillna(0)):
                                relevant.append(col)
                        ticker_weekly = ticker_weekly[relevant]
                        ticker_weekly["ticker"] = ticker
                        ticker_weekly["date"] = [datetime.strptime(f'{row[1]["year"]} {row[1]["week"]} 0', "%Y %W %w") for row in ticker_weekly.iterrows()]
                        ticker_weekly["categories"] = num_cats
                        self.db.store("transformed",ticker_weekly)
                        weekly_sets.append(ticker_weekly)
                except Exception as e:
                    print(phase,category,str(e))
            self.db.disconnect()
            market.disconnect()
            data = pd.concat(weekly_sets)
            self.transformed = True
        return data
    
    def rec_transform(self):
        year = datetime.now().year
        self.db.connect()
        data = self.db.query("rec_transformed",{"categories":self.modeling_params["categories"],
                                                "year":{"$gte":year-self.modeling_params["model_training_year"]-1}
                                                })
        market = self.subscriptions["market"]["db"]
        stock_category = self.subscriptions["stock_category"]["db"]
        if data.index.size > 1:
            self.transformed = True
            return data
        else:
            phase = "initial loads"
            self.db.connect()
            market = self.subscriptions["market"]["db"]
            stock_category = self.subscriptions["stock_category"]["db"]
            market.connect()
            stock_category.connect()
            categories = stock_category.retrieve("sim")
            sp5 = market.retrieve("sp500")
            weekly_gap = 1
            weekly_sets = []
            num_cats = self.modeling_params["categories"]
            categories = categories[categories["year"] >= year - self.modeling_params["model_training_year"] - 1]
            for category in tqdm(categories[f"{num_cats}_classification"].unique()):
                try:
                    cols = categories[categories[f"{num_cats}_classification"]==category]["ticker"].unique()
                    stuff = []
                    phase = "first concat"
                    for t in cols:
                        ticker_data = market.retrieve_ticker_prices("prices",t)
                        ticker_data = p.column_date_processing(ticker_data)
                        stuff.append(ticker_data)
                    heh = pd.concat(stuff)
                    lel = heh.pivot_table(index="date",columns="ticker",values="adjclose").reset_index()
                    lel["year"] = [x.year for x in lel["date"]]
                    lel["week"] = [x.week for x in lel["date"]]
                    lel["quarter"] = [x.quarter for x in lel["date"]]
                    weekly = lel.groupby(["year","quarter","week"]).mean().reset_index()
                    weekly = weekly[weekly["year"] >= year - self.modeling_params["model_training_year"] - 1]
                    for ticker in cols:
                        ticker_weekly = weekly.copy()
                        ticker_weekly["y"] = ticker_weekly[ticker].shift(-weekly_gap)
                        ticker_weekly = ticker_weekly[:-1].copy()
                        relevant = ["year","quarter","week","y"]
                        phase = "finding relevant columns"
                        for col in cols:
                            if 0.0 not in list(ticker_weekly[col].fillna(0)) and col != "y":
                                relevant.append(col)
                        ticker_weekly = ticker_weekly[relevant]
                        ticker_weekly["ticker"] = ticker
                        ticker_weekly["date"] = [datetime.strptime(f'{row[1]["year"]} {row[1]["week"]} 0', "%Y %W %w") for row in ticker_weekly.iterrows()]
                        ticker_weekly["categories"] = num_cats
                        self.db.store("rec_transformed",ticker_weekly)
                        weekly_sets.append(ticker_weekly)
                except Exception as e:
                    print(phase,category,str(e))
            self.db.disconnect()
            market.disconnect()
            data = pd.concat(weekly_sets)
            self.transformed = True
        return data

    def create_sim(self):
        self.db.connect()
        sim = self.db.retrieve_sim(self.modeling_params)
        self.db.disconnect()
        if sim.index.size > 1:
            self.simmed = True
            return sim
        else:
            start_year = self.start_date.year
            end_year = self.end_date.year
            categories_nums = self.modeling_params["categories"]
            model_training_year = self.modeling_params["model_training_year"]
            sims = []
            market = self.subscriptions["market"]["db"]
            market.connect()
            sp5 = market.retrieve("sp500")
            self.db.connect()
            for year in tqdm(range(start_year,end_year),desc="competition_sim_year"):
                for quarter in tqdm(range(1,5),desc="competition_sim_quarter"):
                    for ticker in tqdm(list(sp5["Symbol"].unique()),desc="competition_sim_ticker"):
                        try:
                            model_data = self.db.retrieve_transformed(ticker)
                            model_data.sort_values("date",ascending=True,inplace=True)
                            model_data.reset_index(inplace=True,drop=True)
                            first_index = model_data[(model_data["year"] == (year - model_training_year - 1)) & (model_data["quarter"]==quarter)].index.values.tolist()[0]
                            last_index = model_data[(model_data["year"] == year) & (model_data["quarter"]==quarter)].index.values.tolist()[0]
                            training_data = model_data.iloc[first_index:last_index].reset_index(drop=True)
                            prediction_data = model_data[(model_data["year"] == year) & (model_data["quarter"]==quarter)].reset_index()
                            factor_cols = [x for x in training_data.columns if x not in ["year","quarter","week","y","ticker","date"]]
                            factors= []
                            for factor in factor_cols:
                                if len([x for x in training_data[factor].isna() if x]) / training_data.index.size < 0.2:
                                    factors.append(factor)
                            if len(factors) > 0:
                                X = training_data[factors].fillna(method="ffill").fillna(method="bfill")
                                y = training_data["y"].fillna(method="ffill").fillna(method="bfill")
                                models = m.regression({"X":X,"y":y})
                                models["year"] = year
                                models["quarter"] = quarter
                                sim = prediction_data.fillna(method="ffill").fillna(method="bfill")
                                for i in range(models.index.size):
                                    model = models.iloc[i]
                                    api = model["api"]
                                    score = model["score"]
                                    if score >= self.modeling_params["score_requirement"]/100:
                                        sim[f"{api}_prediction"] = model["model"].predict(sim[factors])
                                        sim[f"{api}_score"] = model["score"].item()
                                ticker_data = market.retrieve_ticker_prices("prices",ticker)
                                prices = p.column_date_processing(ticker_data)
                                prices["year"] = [x.year for x in prices["date"]]
                                prices["week"] = [x.week for x in prices["date"]]
                                sim = p.column_date_processing(sim)
                                sim["year"] = [x.year for x in sim["date"]]
                                sim["week"] = [x.week for x in sim["date"]]
                                sim = prices[["date","year","week","ticker","adjclose"]].merge(sim.drop("date",axis=1),on=["year","week","ticker"],how="right").dropna()
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
                            print(ticker,str(e))
            self.db.disconnect()
            market.disconnect()
            self.simmed = True
        return pd.concat(sims)

    def model(self):
        params = self.modeling_params
        params["year"] = datetime.now().year
        params["quarter"] = (datetime.now().month - 1) // 3 + 1
        if self.ismodeled():
            self.db.connect()
            models = self.db.query("models",params)
            self.db.disconnect()
            return models
        else:
            categories_nums = self.modeling_params["categories"]
            model_training_year = self.modeling_params["model_training_year"]
            sims = []
            market = self.subscriptions["market"]["db"]
            market.connect()
            sp5 = market.retrieve("sp500")
            self.db.connect()
            year = params["year"]
            quarter = params["quarter"]
            for ticker in tqdm(list(sp5["Symbol"].unique()),desc="competition_sim_ticker"):
                try:
                    params["ticker"] = ticker
                    model_data = self.db.query("rec_transformed",{"ticker":ticker})
                    model_data.sort_values("date",ascending=True,inplace=True)
                    model_data.reset_index(inplace=True,drop=True)
                    first_index = model_data[(model_data["year"] == (year - model_training_year - 1)) & (model_data["quarter"]==quarter)].index.values.tolist()[0]
                    last_index = model_data[(model_data["year"] == year) & (model_data["quarter"]==quarter)].index.values.tolist()[0]
                    training_data = model_data.iloc[first_index:last_index].reset_index(drop=True)
                    prediction_data = model_data[(model_data["year"] == year) & (model_data["quarter"]==quarter)].reset_index()
                    factor_cols = [x for x in training_data.columns if x not in ["year","quarter","week","y","ticker","date"]]
                    factors= []
                    for factor in factor_cols:
                        if len([x for x in training_data[factor].isna() if x]) / training_data.index.size < 0.2:
                            factors.append(factor)
                    if len(factors) > 0:
                        X = training_data[factors].fillna(method="ffill").fillna(method="bfill")
                        y = training_data["y"].fillna(method="ffill").fillna(method="bfill")
                        models = m.regression({"X":X,"y":y})
                        models["year"] = year
                        models["quarter"] = quarter
                        models["ticker"] = ticker
                        models["model"] = [pickle.dumps(x) for x in models["model"]]
                        models["factors"] = [factor_cols for i in range(models.index.size)]
                        for param in self.modeling_params:
                            models[param]=self.modeling_params[param]
                        if models.index.size > 1:
                            self.db.store("models",models)
                            sims.append(models)
                except Exception as e:
                    print(ticker,str(e))
            self.db.disconnect()
            market.disconnect()
            self.simmed = True
        return pd.concat(sims)
    
    def recommend(self):
        params = self.modeling_params
        params["year"] = datetime.now().year
        params["quarter"] = (datetime.now().month - 1) // 3 + 1
        params["week"] = datetime.now().isocalendar()[1]
        self.db.connect()
        recs = self.db.query("recs",params)
        self.db.disconnect()
        if recs.index.size > 0:
            return recs
        else:
            if self.ismodeled():
                self.db.connect()
                models = self.db.query("models",params[list(params.keys()[:-1])])
                self.db.disconnect()
            else:
                models = self.model()
            models["model"] = [pickle.loads(x) for x in models["model"]]
            categories_nums = self.modeling_params["categories"]
            model_training_year = self.modeling_params["model_training_year"]
            sims = []
            market = self.subscriptions["market"]["db"]
            market.connect()
            sp5 = market.retrieve("sp500")
            self.db.connect()
            for ticker in tqdm(list(sp5["Symbol"].unique()),desc="competition_sim_ticker"):
                try:
                    params["ticker"] = ticker
                    model_data = self.db.query("rec_transformed",{"ticker":ticker})
                    model_data.sort_values("date",ascending=True,inplace=True)
                    model_data.reset_index(inplace=True,drop=True)
                    model_data.sort_values("date",ascending=True,inplace=True)
                    model_data.reset_index(inplace=True,drop=True)
                    prediction_data = model_data[(model_data["year"] == year) & (model_data["quarter"]==quarter)].reset_index()
                    sim = prediction_data.fillna(method="ffill").fillna(method="bfill")
                    ticker_models = models[models["ticker"]==ticker]
                    for i in range(ticker_models.index.size):
                        model = ticker_models.iloc[i]
                        api = model["api"]
                        score = model["score"]
                        if score >= self.modeling_params["score_requirement"]/100:
                            sim[f"{api}_prediction"] = model["model"].predict(sim[factors])
                            sim[f"{api}_score"] = model["score"].item()
                    sim = p.column_date_processing(sim)
                    sim["year"] = [x.year for x in sim["date"]]
                    sim["week"] = [x.week for x in sim["date"]]
                    sim["categories"] = categories_nums
                    final_cols = ["year","quarter","week","date","ticker","categories"]
                    final_cols.extend([x for x in list(sim.columns) if "prediction" in x or "score" in x])
                    sim = sim[final_cols]
                    sim.fillna(0,inplace=True)
                    sim["prediction"] = [np.nanmean([row[1][x] for x in sim.columns if "prediction" in x and row[1][x] != 0]) for row in sim.iterrows()]
                    sim["score"] = [np.nanmean([row[1][x] for x in sim.columns if "score" in x and row[1][x] != 0]) for row in sim.iterrows()]
                    for param in self.modeling_params:
                        sim[param]=self.modeling_params[param]
                    if sim.index.size > 1:
                        self.db.store("recs",sim)
                        sims.append(sim)
                except Exception as e:
                    print(ticker,str(e))
            self.db.disconnect()
            market.disconnect()
            self.simmed = True
        return pd.concat(sims)