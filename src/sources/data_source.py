from abc import ABC, abstractmethod


class DataSource(ABC):

    @abstractmethod
    def initialize(self) -> None:
        pass

    @abstractmethod
    def has_message(self) -> bool:
        pass

    @abstractmethod
    def read(self) -> dict:
        pass

    @abstractmethod
    def close(self) -> None:
        pass
