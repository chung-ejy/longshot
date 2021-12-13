import pandas as pd
from product.anaiproduct import AnAIProduct
from datetime import timedelta
import pytz
from tqdm import tqdm
pd.options.mode.chained_assignment = None
from modeler.modeler import Modeler as m
from datetime import datetime, timedelta, timezone
import numpy as np
import math
import pickle
from sklearn.preprocessing import OneHotEncoder

class StockCategory(AnAIProduct):
    def __init__(self,params):
        super().__init__("stock_category",
                        {"market":{"preload":True,"tables":{"prices":pd.DataFrame([{}]),"sp500":pd.DataFrame([{}])}},    
                        "sec":{"preload":False,"tables":{"filings":pd.DataFrame([{}])}}}
                        ,params)
    @classmethod
    def required_params(self):
        required = {"training_years":5}
        return required    
    
    def initial_transform(self):
        if self.transformed:
            self.db.connect()
            data = self.db.retrieve("transformed")
            self.db.disconnect()
            return data
        else:
            prices = self.subscriptions["market"]["tables"]["prices"].copy()
            sp5 = self.subscriptions["market"]["tables"]["sp500"].copy()
            prices["date"] = pd.to_datetime(prices["date"])
            prices["year"] = [x.year for x in prices["date"]]
            prices["quarter"] = [x.quarter for x in prices["date"]]
            quarterly_grouped = prices.groupby(["year","quarter","ticker"]).mean().reset_index()
            categories = []
            for value in quarterly_grouped["adjClose"]:
                if value > 0 and value <= 100:
                    categories.append(100)
                else:
                    if value > 100 and value <= 200:
                        categories.append(200)
                    else:
                        categories.append(500)
            quarterly_grouped["category"] = categories
            quarterly_grouped.reset_index(inplace=True)
            groups = quarterly_grouped.merge(sp5.rename(columns={"Symbol":"ticker"}),on="ticker",how="left")
            g = groups[["year","quarter","ticker","adjClose","category","GICS Sector","CIK"]]
            g.dropna(inplace=True)
            g["string_category"]  = [str(x) for x in g["category"]]
            g["classification"] = g["string_category"] + g["GICS Sector"]
            numberss = len(g["classification"].unique())
            enc = OneHotEncoder(handle_unknown="ignore")
            transformed = [[x] for x in g["classification"]]
            encoding = enc.fit_transform(transformed)
            self.db.connect()
            self.db.store("model",pd.DataFrame([{"model":pickle.dumps(enc),"purpose":"encoder"}]))
            self.db.disconnect()
            df_encoding = pd.DataFrame(encoding.toarray())
            for col in df_encoding.columns:
                g[col] = df_encoding[col]   
            fails = []
            cleaned_filings = []
            columns = []
            sec = self.subscriptions["sec"]["db"]
            sec.connect()
            for cik in tqdm(list(g["CIK"].unique())):
                try:
                    filing = sec.retrieve_filing_data(cik)
                    symbols = sp5[sp5["CIK"]==cik]["Symbol"]
                    if symbols.index.size > 1:
                        ticker = str(list(symbols)[0])
                    else:
                        ticker = symbols.item()
                    funds = filing.copy()
                    drop_columns = ["adsh","cik","_id"]
                    for column in funds.columns:
                        if str(column).islower() and str(column) != "filed" and str(column) not in ["year","quarter","ticker"]:
                            drop_columns.append(column)
                    funds["filed"] = [datetime.strptime(str(x),"%Y%m%d").replace(tzinfo=timezone.utc) if "-" not in str(x) else \
                                    datetime.strptime(str(x).split(" ")[0],"%Y-%m-%d").replace(tzinfor=timezone.utc) for x in funds["filed"]]
                    funds["quarter"] = [x.quarter for x in funds["filed"]]
                    funds["year"] = [x.year for x in funds["filed"]]
                    funds["ticker"] = ticker
                    funds.drop(drop_columns,axis=1,inplace=True,errors="ignore")
                    qa = funds.copy()
                    for col in qa.columns:
                        test = qa[col].fillna(-99999)
                        availability = 1 - (len([x for x in test if x == -99999]) / qa.index.size)
                        if availability < 0.95:
                            funds.drop(col,inplace=True,axis=1)
                    cleaned_filings.append(funds)
                except Exception as e:
                    fails.append([ticker,str(e)])
            sec.disconnect()
            try:
                f = pd.concat(cleaned_filings)
                for col in tqdm(f.columns):
                    test = f[col].fillna(-99999)
                    availability = len([x for x in test != -99999 if x == True]) / test.index.size
                    if availability < 0.7:
                        f.drop(col,axis=1,inplace=True)
            except Exception as e:
                print(str(e))
            try:
                data = f.merge(g.drop(["string_category","classification","adjClose","category","GICS Sector","CIK"],axis=1), \
                            on=["year","quarter","ticker"],how="left")
                factors = list(data.columns)
                factors = [x for x in factors if x not in ["year","quarter","ticker"]]
                for i in range(numberss):
                    factors.remove(i)
                for col in factors:
                    data[col].replace([np.inf,-np.inf,np.nan,np.NaN],f[col].mean(),inplace=True)
            except Exception as e:
                print(str(e))
            for col in data.columns:
                data.rename(columns= {col:str(col)},inplace=True)
            data.drop(["_id","filed"],axis=1,inplace=True,errors="ignore")
            for param in self.params:
                data[param] = self.params[param]
            self.db.connect()
            self.db.store("transformed",data)
            self.db.disconnect()
        return data
    
    def create_training_set(self,data,year,training_years,yearly_gap):
        try:
            training_data = data[(data["year"] <= year) & (data["year"] >= year - training_years - 1)]
            factors = list(training_data.columns)
            factors = [x for x in factors if x not in ["year","quarter","ticker","filed"]]
            numberss = max([x for x in range(len(list(training_data.columns))) if str(x) in list(training_data.columns)])
            for i in range(numberss+1):
                try:
                    factors.remove(str(i))
                except:
                    continue
            for i in range(numberss+1):
                try:
                    factors.remove(str(i))
                except:
                    continue
            training_data[factors].fillna(training_data[factors].mean(numeric_only=True),inplace=True)
            training_data.dropna(inplace=True)
            ## resetting the factor reporting dates
            temp = [x for x in factors]
            temp.extend(["year","quarter","ticker"])
            x_plz = training_data[temp]
            x_plz["year"] = x_plz["year"] + yearly_gap
            temp = [str(x) for x in range(numberss+1)]
            temp.extend(["year","quarter","ticker"])
            y_plz = training_data[temp]
            training_data = x_plz.merge(y_plz,on=["year","quarter","ticker"],how="left")
            ## remerging
            relevant = training_data[(training_data["year"] < year) & (training_data["year"] >= year - training_years - 1)]
            relevant.dropna(inplace=True)
            x = relevant[factors]
            y = relevant[[str(x) for x in range(numberss+1)]]
            refined_data = {"X":x.reset_index(drop=True),"y":y.reset_index(drop=True)}
            return refined_data
        except Exception as e:
            print(str(e))
            return training_data
    
    def create_prediction_set(self,data,year,training_years,yearly_gap):
        try:
            training_data = data[(data["year"] <= year) & (data["year"] >= year - training_years - 1)]
            factors = list(training_data.columns)
            factors = [x for x in factors if x not in ["year","quarter","ticker","filed"]]
            numberss = max([x for x in range(len(list(training_data.columns))) if str(x) in list(training_data.columns)])
            for i in range(numberss+1):
                try:
                    factors.remove(str(i))
                except:
                    continue
            for i in range(numberss+1):
                try:
                    factors.remove(str(i))
                except:
                    continue
            training_data[factors].fillna(training_data[factors].mean(numeric_only=True),inplace=True)
            training_data.dropna(inplace=True)
            # resetting the factor reporting dates
            temp = [x for x in factors]
            temp.extend(["year","quarter","ticker"])
            x_plz = training_data[temp]
            x_plz["year"] = x_plz["year"] + yearly_gap
            temp = [str(x) for x in range(numberss+1)]
            temp.extend(["year","quarter","ticker"])
            y_plz = training_data[temp]
            training_data = x_plz.merge(y_plz,on=["year","quarter","ticker"],how="left")
            ## remerging
            prediction_data = training_data[training_data["year"]==year]
            return prediction_data
        except Exception as e:
            print(str(e))
            return training_data

    
    def create_sim(self):
        self.db.connect()
        data = self.db.retrieve("transformed")
        self.db.disconnect()
        start = data["year"].min()
        end = data["year"].max()
        yearly_gap = 1
        training_years = self.params["training_years"]
        for year in tqdm(range(start,end)):
            try:
                prediction_data = self.create_prediction_set(data,year,training_years,yearly_gap)
                refined_data = self.create_training_set(data,year,training_years,yearly_gap)
                models = m.xgb_classify(refined_data.copy(),multioutput=True)
                model = models["model"]
                factors = list(refined_data["X"].columns)
                self.db.connect()
                hmodels = self.db.retrieve("model")
                encoder = pickle.loads(hmodels[hmodels["purpose"]=="encoder"]["model"].item())
                self.db.disconnect()
                prediction_data[factors].fillna(prediction_data[factors].mean(numeric_only=True),inplace=True)
                predictions = encoder.inverse_transform(model.predict(prediction_data[factors]))
                prediction_data["prediction"] = [x[0] for x in predictions]
                prediction_data["score"] = models["score"].item()
                sim = prediction_data[["year","quarter","ticker","prediction","score"]]
                sim["training_years"] = training_years
                self.db.connect()
                self.db.store("sim",sim)
                self.db.disconnect()
            except Exception as e:
                print(year,str(e))