"""
PySpark Dataframe.

Output format returning a PySpark Dataframe
"""
from pyspark.sql import SparkSession, Row
from .base import OutputFormat

class pysparkFormat(OutputFormat):
    """
    Most Basic Definition of Format.
    """
    def __init__(self, **kwargs) -> None:
        if 'appName' in kwargs:
            self._spark = SparkSession.builder.appName(kwargs['appName']).getOrCreate()
        else:
            self._spark = SparkSession.builder.getOrCreate()
        super(pysparkFormat, self).__init__()

    async def serialize(self, result, error, *args, **kwargs):
        if isinstance(result, list):
            row = result[0]
        else:
            row = result
        if not row:
            raise ValueError(
                f"PySpark Format Error: invalid Resulset: {result!r}"
            )
        try:
            columns = list(row.keys())
            data = [tuple(v) for v in result]
            # rdd = self._spark.sparkContext.parallelize(data)
            df = self._spark.createDataFrame(data).toDF(*columns)
            #df = rdd.toDF(columns)
            return (df, error)
        except (ValueError, TypeError) as e:
            raise RuntimeError(
                f"PySpark Output Error: {e}"
            ) from e
