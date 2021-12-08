import pandas as pd
from strategy.anaistrategy import AnAIStrategy
from datetime import timedelta, datetime
import pytz
from tqdm import tqdm
pd.options.mode.chained_assignment = None
from modeler.modeler import Modeler as m

class Speculation(AnAIStrategy):
    def __init__(self,start_date,end_date,params=
                        {"timeframe":"weekly"
                    ,"score_requirement":70
                    ,"requirement":5
                    ,"category_training_year":4
                    ,"model_training_year":1
                    ,"number_of_training_weeks":14
                    ,"value":True}):
        super().__init__("speculation",
                            start_date,
                            end_date,
                        {"market":
                            {   "preload":True,
                                "tables":
                                { "prices":None,
                                "sp500":None}
                            },
                        "stock_category":{
                            "preload":True,
                            "tables":{
                                "sim":None
                            }
                        }},
                        params
                     )
        self.transformed = False
    @classmethod
    def required_params(self):
        required =  {"timeframe":"weekly"
                    ,"requirement":5
                    ,"score_requirement":70
                    ,"number_of_training_weeks":14
                    ,"value":True}
        return required

    def initial_transform(self):
        self.db.connect()
        data = self.db.retrieve("transformed")
        self.db.disconnect()
        categories = self.subscriptions["stock_category"]["tables"]["sim"]
        prices = self.subscriptions["market"]["tables"]["prices"]
        sp5 = self.subscriptions["market"]["tables"]["sp500"]
        start = self.start_date.year
        end = self.end_date.year
        prices["date"] = pd.to_datetime(prices["date"])
        prices["year"] = [x.year for x in prices["date"]]
        prices["quarter"] = [x.quarter for x in prices["date"]]
        prices["week"] = [x.week for x in prices["date"]]
        weekly_gap = 1
        number_of_training_weeks = self.params["number_of_training_weeks"]
        weekly_sets = []
        if self.transformed or data.index.size > 1:
            self.transformed = True
            return data
        else:
            prices = prices[(prices["year"] >= start - self.params["model_training_year"] - 1) & (prices["year"] <= end)]
            for ticker in tqdm(sp5["Symbol"].unique()):
                ticker_data = prices[prices["ticker"]==ticker]
                weekly = ticker_data.groupby(["year","quarter","week"]).mean().reset_index()
                for i in range(number_of_training_weeks):
                    weekly[i] = weekly["adjClose"].shift(i)
                weekly["y"] = weekly["adjClose"].shift(-1)
                weekly.dropna(inplace=True)
                weekly["ticker"] = ticker
                weekly_sets.append(weekly)
            data = pd.concat(weekly_sets)
            for i in range(number_of_training_weeks):
                data.rename(columns={i:str(i)},inplace=True)
            data["date"] = [datetime.strptime(f'{row[1]["year"]} {row[1]["week"]} 0', "%Y %W %w") for row in data.iterrows()]
            for param in self.params:
                data[param] = self.params[param]
            self.db.connect()
            self.db.store("transformed",data)
            self.db.disconnect()
            self.transformed = True
        return data

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
            self.db.connect()
            data = self.db.retrieve("transformed")
            self.db.disconnect()
            for col in self.params.keys():
                data.drop(col,axis=1,inplace=True)
            categories = self.subscriptions["stock_category"]["tables"]["sim"]
            category_training_year = self.params["category_training_year"]
            subset_categories = categories[categories["training_years"]==category_training_year]
            number_of_training_weeks = self.params["number_of_training_weeks"]
            model_training_year = self.params["model_training_year"]
            self.db.connect()
            sims = []
            for year in range(start_year,end_year+1):
                for quarter in range(1,5):
                    quarterly_categories = subset_categories[(subset_categories["year"]==year) & (subset_categories["quarter"]==quarter)]
                    for category in quarterly_categories["prediction"].unique():
                        try:
                            category_tickers = quarterly_categories[quarterly_categories["prediction"]==category]["ticker"].unique()
                            model_data = data[(data["ticker"].isin(category_tickers))]
                            model_data.sort_values("date",ascending=True,inplace=True)
                            first_index = model_data[(model_data["year"] == year - model_training_year - 1) & (model_data["quarter"]==quarter)].index.values.tolist()[0]
                            last_index = model_data[(model_data["year"] == year) & (model_data["quarter"]==quarter)].index.values.tolist()[0]
                            training_data = model_data.iloc[first_index:last_index]
                            prediction_data = model_data[(model_data["year"] == year) & (model_data["quarter"]==quarter)]
                            X = training_data[[str(x) for x in range(number_of_training_weeks)]]
                            y = training_data["y"]
                            xgb_models = m.xgb_regression({"X":X,"y":y})
                            sk_models = m.sk_regression({"X":X,"y":y})
                            sk_models.append(xgb_models)
                            models = pd.DataFrame(sk_models)
                            model = models.sort_values("score",ascending=False).iloc[0]
                            sim = prediction_data
                            sim["weekly_price_regression_prediction"] = model["model"].predict(sim[[str(x) for x in range(number_of_training_weeks)]])
                            sim["score"] = model["score"].item()
                            sim = sim.groupby(["year","week","ticker"]).mean().reset_index()
                            sim = prices[["date","year","week","ticker","adjClose"]].merge(sim[["year","week","ticker","weekly_price_regression_prediction","score"]],how="left",on=["year","week","ticker"])
                            sim["delta"] = (sim["weekly_price_regression_prediction"] - sim["adjClose"]) / sim["adjClose"]
                            sim = sim[["date","ticker","adjClose","delta","score"]].dropna()
                            for param in self.params:
                                sim[param]=self.params[param]
                            self.db.store("sim",sim)
                            sims.append(sim)
                        except Exception as e:
                            print(year,quarter,str(e))
            self.db.disconnect()
            self.simmed = True
        return pd.concat(sims)
               
    def daily_recommendation(self,date,sim,seat):
        if not self.params["value"]:
            sim["delta"] = sim["delta"] * -1
        while date.weekday() > 4:
            date = date + timedelta(days=1)
        try:
            daily_rec = sim[(sim["date"]>=date) & 
                        (sim["delta"] >= float(self.params["requirement"]/100))]
        except:
            daily_rec = sim[(sim["date"]>=date) & 
                        (sim["delta"] >= float(self.params["requirement"]/100))]
        daily_rec = daily_rec[daily_rec["date"]==daily_rec["date"].min()].sort_values("delta",ascending=False)
        try:
            if daily_rec.index.size >= seat:
                result = daily_rec[["date","adjClose","ticker"]].iloc[seat].to_dict()
                result["seat"] = seat
                return result
            else:
                return {"error":"no trade","date":date}
        except Exception as e:
            return {"error":str(e)}
    
    def exit(self,sim,trade):
        bp = trade["adjClose"]
        sp = trade["adjClose"] * float(1+(self.params["requirement"]/100.0))
        try:
            this_exit = sim[(sim["date"] > trade["date"]) & (sim["adjClose"]>=sp)
            & (sim["ticker"]==trade["ticker"])].sort_values("date").iloc[0]
        except:
            this_exit = sim[(sim["date"] > trade["date"]) & (sim["adjClose"]>=sp)
            & (sim["ticker"]==trade["ticker"])].sort_values("date").iloc[0]
        trade["sell_date"] = this_exit["date"]
        trade["sell_price"] = sp
        return trade
        
