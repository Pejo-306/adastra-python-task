from abc import ABC, abstractmethod


class DataSource(ABC):

    @abstractmethod
    def __enter__(self):
        pass

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

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
