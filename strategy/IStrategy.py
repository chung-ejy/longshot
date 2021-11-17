from abc import ABCMeta, abstractmethod

## sources https://realpython.com/python-interface/
## https://docs.python.org/3/library/abc.html
class IStrategy(metaclass=ABCMeta):
    @classmethod
    def __subclasshook_(cls,subclass):
        return (hasattr(subclass,"load") and callable(subclass.load) 
                and hasattr(subclass,"transform") and callable(subclass.transform)
                # and hasattr(subclass,"store_model") and callable(subclass.store_model)
                and hasattr(subclass,"backtest") and callable(subclass.backtest))
                and hasattr(subclass,"predict") and callable(subclass.predict))
    
    @abstractmethod
    def load(self):
        raise NotImplementedError("must define load")
    
    @abstractmethod
    def create_training_set(self):
        raise NotImplementedError("must define create_training_set")
    
    @abstractmethod
    def create_prediction_set(self):
        raise NotImplementedError("must define create_prediction_set")
    
    @abstractmethod
    def create_sim(self):
        raise NotImplementedError("must define create_sim")
    
    @abstractmethod
    def create_record(self):
        raise NotImplementedError("must define create_record")

    @abstractmethod
    def daily_recommendation(self):
        raise NotImplementedError("must define daily_recommendation")
