from abc import ABCMeta, abstractmethod

## sources https://realpython.com/python-interface/
## https://docs.python.org/3/library/abc.html
class IDatabase(metaclass=ABCMeta):
    @classmethod
    def __subclasshook_(cls,subclass):
        return (hasattr(subclass,"store") and callable(subclass.store)
                and hasattr(subclass,"retrieve") and callable(subclass.retrieve))
    
    @abstractmethod
    def store(self):
        raise NotImplementedError("must define store")
    
    @abstractmethod
    def retrieve(self):
        raise NotImplementedError("must define retrieve")