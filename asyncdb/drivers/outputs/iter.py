"""
Iterable.

Output format returning a simple list of dictionaries.
"""
from .base import OutputFormat


class iterFormat(OutputFormat):
    """
    Most Basic Definition of Format.
    """
    async def serialize(self, result, error, *args, **kwargs):
        try:
            if isinstance(result, list):
                data = [dict(row) for row in result]
            else:
                data = dict(result)
        except (ValueError, TypeError):
            return (result, error)
        return (data, error)
