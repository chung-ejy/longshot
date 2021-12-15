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
        data = self.db.retrieve("transformed")
        market = self.subscriptions["market"]["db"]
        stock_category = self.subscriptions["stock_category"]["db"]
        if self.transformed or data.index.size > 1:
            self.transformed = True
            return data
        else:
            self.db.connect()
            market = self.subscriptions["market"]["db"]
            stock_category = self.subscriptions["stock_category"]["db"]
            market.connect()
            stock_category.connect()
            categories = stock_category.retrieve("sim")
            sp5 = market.retrieve("sp500")
            weekly_gap = 1
            weekly_sets = []
            for ticker in tqdm(sp5["Symbol"].unique(),desc="competition_transformations"):
                try:
                    num_cats = self.modeling_params["categories"]
                    category = categories[categories["ticker"]==ticker][f"{num_cats}_classification"].iloc[0]
                    cols = categories[categories[f"{num_cats}_classification"]==category]["ticker"].unique()
                    stuff = []
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
                    weekly = weekly[weekly["year"] > 2010]
                    weekly["y"] = weekly[ticker].shift(-weekly_gap)
                    weekly = weekly[:-1]
                    relevant = ["year","quarter","week","y"]
                    for col in cols:
                        if 0.0 not in list(weekly[col].fillna(0)):
                            relevant.append(col)
                    weekly = weekly[relevant]
                    weekly["ticker"] = ticker
                    weekly["date"] = [datetime.strptime(f'{row[1]["year"]} {row[1]["week"]} 0', "%Y %W %w") for row in weekly.iterrows()]
                    self.db.store("transformed",weekly)
                    weekly_sets.append(weekly)
                except Exception as e:
                    print(ticker,str(e))
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
            self.db.connect()
            data = self.db.retrieve("transformed")
            self.db.disconnect()
            stock_category = self.subscriptions["stock_category"]["db"]
            stock_category.connect()
            categories = stock_category.retrieve("sim")
            stock_category.disconnect()
            number_of_training_weeks = self.modeling_params["number_of_training_weeks"]
            model_training_year = self.modeling_params["model_training_year"]
            categories_nums = self.modeling_params["categories"]
            sims = []
            market = self.subscriptions["market"]["db"]
            market.connect()
            self.db.connect()
            for year in tqdm(range(start_year,end_year+1),desc="competition_sim_year"):
                for quarter in range(1,5):
                    quarterly_categories = categories[(categories["year"]==year) & (categories["quarter"]==quarter)]
                    for category in list(quarterly_categories[f"{categories_nums}_classification"].unique()):
                        try:
                            category_tickers = quarterly_categories[quarterly_categories[f"{categories_nums}_classification"]==category]["ticker"].unique()
                            model_data = data[(data["ticker"].isin(category_tickers))]
                            model_data.sort_values("date",ascending=True,inplace=True)
                            model_data.reset_index(inplace=True)
                            first_index = model_data[(model_data["year"] == (year - model_training_year - 1)) & (model_data["quarter"]==quarter)].index.values.tolist()[0]
                            last_index = model_data[(model_data["year"] == year) & (model_data["quarter"]==quarter)].index.values.tolist()[0]
                            training_data = model_data.iloc[first_index:last_index].reset_index()
                            prediction_data = model_data[(model_data["year"] == year) & (model_data["quarter"]==quarter)].reset_index()
                            # print(year,quarter,category,len(category_tickers),training_data.index.size,prediction_data.index.size)
                            X = training_data[[str(x) for x in range(number_of_training_weeks)]]
                            y = training_data["y"]
                            models = m.regression({"X":X,"y":y})
                            models["year"] = year
                            models["quarter"] = quarter
                            models["category"] = category
                            sim = prediction_data
                            for i in range(models.index.size):
                                model = models.iloc[i]
                                api = model["api"]
                                score = model["score"]
                                if score >= self.modeling_params["score_requirement"]/100:
                                    sim[f"{api}_prediction"] = model["model"].predict(sim[[str(x) for x in range(number_of_training_weeks)]])
                                    sim[f"{api}_score"] = model["score"].item()
                            stuff = []
                            for ticker in category_tickers:
                                ticker_data = market.retrieve_ticker_prices("prices",ticker)
                                stuff.append(ticker_data)
                            prices = p.column_date_processing(pd.concat(stuff))
                            prices["year"] = [x.year for x in prices["date"]]
                            prices["week"] = [x.week for x in prices["date"]]
                            sim = p.column_date_processing(sim)
                            sim["year"] = [x.year for x in sim["date"]]
                            sim["week"] = [x.week for x in sim["date"]]
                            sim = prices[["date","year","week","ticker","adjclose"]].merge(sim.drop(["date","adjclose"],axis=1),on=["year","week","ticker"],how="right").dropna()
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
                            print(category,str(e))
            self.db.disconnect()
            market.disconnect()
            self.simmed = True
        return pd.concat(sims)
               
    def daily_recommendation(self,date,sim,seat):
        try:
            daily_rec = sim[(sim["date"]>=date) & 
                        (sim["delta"] >= float(self.trading_params["requirement"]/100))]
        except:
            daily_rec = sim[(sim["date"]>=date) & 
                        (sim["delta"] >= float(self.trading_params["requirement"]/100))]
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
        sp = trade["adjclose"] * float(1+(rp.trading_params["requirement"]/100.0))
        phase = "exiting"
        exits = sim[(sim["date"] > trade["date"]) & (sim["adjclose"]>=sp)
            & (sim["ticker"]==trade["ticker"])].sort_values("date")
        if exits.index.size < 1:
            exits = sim[(sim["date"] > trade["date"]) & (sim["ticker"]==trade["ticker"])].sort_values("date",ascending=False)
            if exits.index.size < 1:
                date = date + timedelta(days=1)
            else:
                the_exit = exits.iloc[0]
        else:
            the_exit = exits.iloc[0]
        trade["sell_date"] = the_exit["date"]
        trade["sell_price"] = sp
        trade = {**trade,**rp.trading_params}
        return trade
        