from abc import ABC, abstractmethod


class OutputFormat(ABC):
    """
    Abstract Interface for different output formats.
    """

    @abstractmethod
    async def serialize(self, *args, **kwargs):
        """
        Making the serialization
        """
        pass

    async def __call__(self, result, error, *args, **kwargs):
        return await self.serialize(result, error, *args, **kwargs)
