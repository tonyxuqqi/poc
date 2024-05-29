from abc import ABC, abstractmethod

class Store(ABC):
    @abstractmethod
    def insert(self, key, value, index_keys):
        pass
    
    @abstractmethod
    def batch_insert(self, keys, values, index_keys_slice):
        pass
    
    @abstractmethod
    def get_by(self, index_key):
        pass

    @abstractmethod
    def multi_get_by(self, index_keys):
        pass
