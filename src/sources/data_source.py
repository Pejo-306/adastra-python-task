from abc import ABC, abstractmethod


class DataSource(ABC):

    @abstractmethod
    def initialize(self):
        pass

    @abstractmethod
    def has_message(self) -> bool:
        pass

    @abstractmethod
    def read(self) -> str:
        pass

    @abstractmethod
    def close(self):
        pass
