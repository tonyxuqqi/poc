from abc import ABC, abstractmethod

class Store(ABC):
    @abstractmethod
    def insert(self, key, value, index_keys):
        pass
    
    @abstractmethod
    def batch_insert(self, keys, values, index_keys_slice):
        pass
