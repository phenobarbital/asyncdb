from ..interfaces.cursors import CursorBackend
from .base import BaseDriver


class BaseCursor(CursorBackend):
    """
    baseCursor.

    Iterable Object for Cursor-Like functionality
    """

    _provider: BaseDriver
