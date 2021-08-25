from math import e
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.metrics import r2_score, accuracy_score
from sklearn.linear_model import LinearRegression, SGDRegressor, RidgeCV, SGDClassifier, RidgeClassifier
from sklearn.preprocessing import LabelEncoder
from sklearn.neighbors import KNeighborsClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier, VotingClassifier
from sklearn.svm import SVC
from sklearn.multioutput import MultiOutputClassifier
from sklearn.naive_bayes import GaussianNB
import pandas as pd
import xgboost as xgb
from xgboost.sklearn import XGBClassifier
from xgboost.training import train

class Modeler(object):

    @classmethod
    def classification(self,data,xgb,sk,multioutput):
        results = []
        data["y"] = data["y"].drop("y",axis=1,errors="ignore")
        if sk:
            sk_result = self.sk_classify(data,multioutput)
            results.append(sk_result)
        if xgb:
            xgb_result = self.xgb_classify(data,multioutput)
            results.append(xgb_result)
        df = pd.DataFrame(results)
        df["model_type"] = "classification"
        return df.sort_values("score",ascending=False).iloc[0]

    @classmethod
    def regression(self,data,xgb,sk):
        results = []
        data["y"] = data["y"].drop("y",axis=1,errors="ignore")
        if sk:
            sk_result = self.sk_regression(data)
            results.append(sk_result)
        if xgb:
            xgb_result = self.xgb_regression(data)
            results.append(xgb_result)
        df = pd.DataFrame(results)
        df["model_type"] = "regression"
        return df.sort_values("score",ascending=False).iloc[0]
    
    @classmethod
    def xgb_regression(self,data):
        try:
            params = {"booster":["gbtree","gblinear","dart"]}
            X_train, X_test, y_train, y_test = self.shuffle_split(data)
            gs = GridSearchCV(xgb.XGBRegressor(objective="reg:squarederror"),param_grid=params,scoring="r2")
            gs.fit(X_train,y_train)
            score = r2_score(gs.predict(X_test),y_test)
            model = gs.best_estimator_
            return {"api":"xgb","model":model,"score":score}
        except Exception as e:
            print(str(e))
    
    @classmethod
    def xgb_classify(self,data,multioutput):
        try:
            params = {"booster":["gbtree","gblinear","dart"]}
            X_train, X_test, y_train, y_test = self.shuffle_split(data)
            if multioutput:
                gs = MultiOutputClassifier(XGBClassifier(eval_metric="logloss"))
                gs.fit(X_train,y_train)
                model = gs
            else:
                gs = GridSearchCV(xgb.XGBClassifier(objective="binary:logistic",eval_metric="logloss"),params_grid=params,scoring="accuracy")
                y_train = LabelEncoder().fit(y_train).transform(y_train)
                gs.fit(X_train,y_train)
                y_test = LabelEncoder().fit(y_test).transform(y_test)
                model = gs.best_estimator_
            score = accuracy_score(model.predict(X_test),y_test)
            return {"api":"xgb","model":model,"score":score}
        except Exception as e:
            print(str(e))
    
    @classmethod
    def sk_regression(self,data):
        stuff = {
            "sgd" : {"model":SGDRegressor(fit_intercept=True),"params":{"loss":["squared_loss","huber"]
                                                            ,"learning_rate":["constant","optimal","adaptive"]
                                                            ,"alpha" : [0.0001,0.001, 0.01, 0.1, 0.2, 0.5, 1]}},
            "r" : {"model":RidgeCV(alphas=[0.0001,0.001, 0.01, 0.1, 0.2, 0.5, 1],fit_intercept=True),"params":{}},
            "lr" : {"model":LinearRegression(fit_intercept=True),"params":{"fit_intercept":[True,False]}}
        }
        X_train, X_test, y_train, y_test = self.shuffle_split(data)
        results = []
        for regressor in stuff:
            try:
                model = stuff[regressor]["model"]
                model.fit(X_train,y_train)
                y_pred = model.predict(X_test)
                score = r2_score(y_test,y_pred)
                result = {"api":"skl","model":model,"score":score}
                results.append(result)
            except Exception as e:
                print(str(e))
                results.append({"api":"skl","model":str(e),"score":-99999})
        return results
    
    def sk_classify(self,data,multioutput):
        results = []
        vc = VotingClassifier(estimators=[("sgdc", SGDClassifier(early_stopping=True)),
                ("ridge", RidgeClassifier()),
                ("tree",DecisionTreeClassifier()),
                ("neighbors",KNeighborsClassifier()),
                ("svc",SVC()),
                ("g",GaussianNB()),
                ("rfc",RandomForestClassifier())])
        stuff = {"sgdc" : {"model":SGDClassifier(),"params":{"loss":["hinge","log","perceptron"]
                                                                                ,"learning_rate":["constant","optimal","adaptive"]
                                                                                ,"alpha" : [0.0001,0.001, 0.01, 0.1, 0.2, 0.5, 1]}},
                "ridge" : {"model":RidgeClassifier(),"params":{"alpha" : [0.0001,0.001, 0.01, 0.1, 0.2, 0.5, 1]}},
                "tree":{"model":DecisionTreeClassifier(),"params":{'max_depth': range(1,11)}},
                "neighbors":{"model":KNeighborsClassifier(),"params":{'knn__n_neighbors': range(1, 10)}},
                "svc":{"model":SVC(),"params":{"kernel":["linear","poly","rbf"],"C":[0.001,0.01,0.1,1,10],"gamma":[0.001,0.01,0.1,1]}},
                "g":{"model":GaussianNB(),"params":{}},
                "rfc":{"model":RandomForestClassifier(),"params":{"criterion":["gini","entropy"]
                                                                ,"n_estimators":[100,150,200]
                                                                ,"max_depth":[None,1,3,5,10]
                                                                ,"min_samples_split":[5,10]
                                                                ,"min_samples_leaf":[5,10]}},
                "vc":vc}
        X_train, X_test, y_train, y_test = self.shuffle_split(data)
        results = []
        for classifier in stuff:
            try:
                model = stuff[classifier]["model"]
                params = stuff[classifier]["params"]
                if classifier == "vc" or not multioutput:
                    model.fit(X_train,y_train)
                if multioutput:
                    model = MultiOutputClassifier(model).fit(X_train,y_train)
                else:
                    gs = GridSearchCV(model,params,cv=5,scoring="accuracy")
                    gs.fit(X_train,y_train)
                    model = gs.best_estimator_
                y_pred = model.predict(X_test)
                score = accuracy_score(y_test,y_pred)
                result = {"api":"skl","model":model,"score":score}
                results.append(result)
            except Exception as e:
                results.append({"api":"skl","model":str(e),"score":-99999})
        return results
    
    @classmethod
    def shuffle_split(self,data):
        return train_test_split(data["X"],data["y"],train_size=0.75,test_size=0.25,random_state=42)