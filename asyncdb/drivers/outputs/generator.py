"""
Generator.

Output format returning a list of dictionaries as a generator
"""
from .base import OutputFormat


class genFormat(OutputFormat):
    """
    Most Basic Definition of Format.
    """
    async def serialize(self, result, error, *args, **kwargs):
        if error:
            return (None, error)
        lsgen = (dict(row) for row in result)
        return (lsgen, error)
