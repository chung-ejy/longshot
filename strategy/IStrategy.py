from abc import ABCMeta, abstractmethod

## sources https://realpython.com/python-interface/
## https://docs.python.org/3/library/abc.html
class IStrategy(metaclass=ABCMeta):
    @classmethod
    def __subclasshook_(cls,subclass):
        return (hasattr(subclass,"subscribe") and callable(subclass.subscribe) 
                and hasattr(subclass,"create_sim") and callable(subclass.create_sim)
                and hasattr(subclass,"daily_recommendation") and callable(subclass.daily_recommendation)
                and hasattr(subclass,"exit") and callable(subclass.exit)
            )
    
    @abstractmethod
    def subscribe(self):
        raise NotImplementedError("must define subscribe")
    
    @abstractmethod
    def create_sim(self):
        raise NotImplementedError("must define create_sim")