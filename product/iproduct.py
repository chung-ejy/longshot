from abc import ABCMeta, abstractmethod

## sources https://realpython.com/python-interface/
## https://docs.python.org/3/library/abc.html
class IProduct(metaclass=ABCMeta):
    @classmethod
    def __subclasshook_(cls,subclass):
        return (
                hasattr(subclass,"load") and callable(subclass.load) 
                and hasattr(subclass,"subscribe") and callable(subclass.subscribe) 
                and hasattr(subclass,"create_sim") and callable(subclass.create_sim)
            )
    
    @abstractmethod
    def subscribe(self):
        raise NotImplementedError("must define subscribe")

    @abstractmethod
    def load(self):
        raise NotImplementedError("must define load")
    
    @abstractmethod
    def create_sim(self):
        raise NotImplementedError("must define create_sim")
