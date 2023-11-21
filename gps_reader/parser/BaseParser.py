from abc import ABC, abstractmethod

class BaseParser(ABC):
    
    @abstractmethod
    def fromfile(self):
        pass
    
    

