from abc import ABCMeta, abstractmethod

## sources https://realpython.com/python-interface/
## https://docs.python.org/3/library/abc.html
class IStrategy(metaclass=ABCMeta):
    @classmethod
    def __subclasshook_(cls,subclass):
        return (hasattr(subclass,"load") and callable(subclass.load) 
                and hasattr(subclass,"transform") and callable(subclass.transform)
                and hasattr(subclass,"store_model") and callable(subclass.store_model)
                and hasattr(subclass,"backtest") and callable(subclass.backtest))
                and hasattr(subclass,"predict") and callable(subclass.predict))
    
    @abstractmethod
    def load(self):
        raise NotImplementedError("must define load")
    
    @abstractmethod
    def transform(self):
        raise NotImplementedError("must define transform")
    
    @abstractmethod
    def store_model(self):
        raise NotImplementedError("must define store_model")
    
    @abstractmethod
    def backtest(self):
        raise NotImplementedError("must define backtest")
    
    @abstractmethod
    def predict(self):
        raise NotImplementedError("must define predict")