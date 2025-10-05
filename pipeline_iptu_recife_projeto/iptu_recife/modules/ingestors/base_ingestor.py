from abc import ABC, abstractmethod
import pandas as pd
from modules.utils.logger import setup_logger # Garanta que este módulo existe

class BaseIngestor(ABC):
    
    def __init__(self, source: BytesIO, filename: str):
        self.logger = setup_logger(self.__class__.__name__)
        
    @abstractmethod
    def read(self, path: str) -> pd.DataFrame:
        pass