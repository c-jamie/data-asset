import abc
from pathlib import Path


class AbstractIOContext(abc.ABC):
    @abc.abstractmethod
    def context(self):
        raise NotImplementedError


class FileIOContext(AbstractIOContext):
    def __init__(self, base_path: str = None):
        self.base_path = Path(base_path)

    def context(self) -> Path:
        """the base path to load

        Returns
        -------
        Path
            the base path
        """
        return self.base_path


class DummyIOContext(AbstractIOContext):
    def __init__(self):
        pass

    def context(self) -> None:
        return None
