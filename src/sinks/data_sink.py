from abc import ABC, abstractmethod


class DataSink(ABC):

    @abstractmethod
    def initialize(self) -> None:
        pass

    @abstractmethod
    def dump(self, message: dict) -> bool:
        pass

    @abstractmethod
    def close(self) -> None:
        pass
