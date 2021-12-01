from abc import ABCMeta, abstractmethod

## sources https://realpython.com/python-interface/
## https://docs.python.org/3/library/abc.html
class IPortfolio(metaclass=ABCMeta):
    @classmethod
    def __subclasshook_(cls,subclass):
        return (
                hasattr(subclass,"load") and callable(subclass.load) 
                and hasattr(subclass,"sim") and callable(subclass.sim) 
                and hasattr(subclass,"backtest") and callable(subclass.backtest)
            )
    
    @abstractmethod
    def load(self):
        raise NotImplementedError("must define load")

    @abstractmethod
    def sim(self):
        raise NotImplementedError("must define sim")
    
    @abstractmethod
    def backtest(self):
        raise NotImplementedError("must define backtest")
