from abc import ABCMeta, abstractmethod

## sources https://realpython.com/python-interface/
## https://docs.python.org/3/library/abc.html
class IStrategy(metaclass=ABCMeta):
    @classmethod
    def __subclasshook_(cls,subclass):
        return (hasattr(subclass,"load") and callable(subclass.load) 
                and hasattr(subclass,"subscribe") and callable(subclass.subscribe) 
                and hasattr(subclass,"create_training_set") and callable(subclass.create_training_set)
                and hasattr(subclass,"create_prediction_set") and callable(subclass.create_prediction_set)
                and hasattr(subclass,"create_sim") and callable(subclass.create_sim)
                and hasattr(subclass,"create_record") and callable(subclass.create_record)
                and hasattr(subclass,"daily_recommendation") and callable(subclass.daily_recommendation)
                )
    
    @abstractmethod
    def subscribe(self):
        raise NotImplementedError("must define subscribe")

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
