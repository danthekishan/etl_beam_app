from abc import ABC, abstractmethod


class BaseTransform(ABC):
    @abstractmethod
    def transform(self, data_line):
        raise NotImplementedError("Transform method has not been implemented")
