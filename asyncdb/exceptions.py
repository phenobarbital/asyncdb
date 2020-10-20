class asyncDBException(Exception):
    """Base class for other exceptions"""

    def __init__(self, message="", code=0, *args):
        super(asyncDBException, self).__init__(*args)
        self.args = (
            message,
            code,
        )
        self.message = message
        self.code = code
        print(args)

    def __str__(self):
        return "{} Error Code: {}".format(self.message, self.code)

    def get(self):
        return self.message


class DataError(asyncDBException, ValueError):
    """An error caused by invalid query input."""


class NotSupported(asyncDBException):
    """Not Supported functionality"""


class ProviderError(asyncDBException):
    """Database Provider Error"""


class NotImplementedError(asyncDBException):
    """Exception for Not implementation"""


class UninitializedError(ProviderError):
    """Exception when provider cant be initialized"""


class ConnectionError(ProviderError):
    """Generic Connection Error"""


class ConnectionTimeout(ProviderError):
    """Connection Timeout Error"""


class NoDataFound(ProviderError):
    """Raise when no data was found"""


class TooManyConnections(ProviderError):
    """Too Many Connections"""


class EmptyStatement(asyncDBException):
    """Raise when no Statement was found"""


class UnknownPropertyError(ProviderError):
    """Raise when invalid property was provide"""


class StatementError(ProviderError):
    """Raise when an Statement Error"""


class ConditionsError(ProviderError):
    """Raise when Failed Conditions"""
