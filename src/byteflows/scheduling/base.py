from abc import abstractmethod

from byteflows.core import ByteflowCore

__all__ = ["ActionCondition", "BaseLimit"]


class ActionCondition(ByteflowCore):
    """
    The base class for all subclasses that control the frequency of data collection. Subclasses of the ActionCondition
    class define a control event that causes requests to be sent to a physical resource by data collectors. Currently
    only scheduling based on calendar events and time is supported.
    """

    @abstractmethod
    async def pending(self) -> None:
        """
        The method starts waiting for the occurrence of a control event, checking it in an infinite loop until the condition becomes true.
        """
        while not self.is_able():
            ...

    @abstractmethod
    def is_able(self) -> bool:
        """
        This method implements the control event verification logic.

        Returns:
            bool: returns True if the control event occurred.
        """
        ...


class BaseLimit(ByteflowCore):
    """
    Limit classes are a special type of scheduler intended exclusively for use in storage classes.
    Limits analyze the specified storage indicators and signal when a specified threshold is exceeded
    (for example, the amount of memory occupied, the number of elements, etc.).
    In storage they are used to dump data from the buffer to the backend.
    """

    @abstractmethod
    def is_overflowed(self) -> bool:
        """
        The method implements specific logic for checking storage performance.

        Returns:
            bool: returns True if the threshold has been passed.
        """
        ...
