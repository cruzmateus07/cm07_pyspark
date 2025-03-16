from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
import pyspark.sql.functions as func


class SparkDfUtils():
    """
        A class with often used DataFrame manipulation functions
    """

    def __init__(self, spark: SparkSession):
        self._spark = spark

    def get_last_update_ts(self, df: DataFrame, ts_field: str):
        """
            Gets the latest timestamp on a given column on the DataFrame

            Parameters:
                df (pyspark.sql.DataFrame): A PySpark DataFrame
                ts_field (str): A timestamp column where the latest timestamp is to be found

            Returns:
                last_update_ts (str): Latest timestamp from the given column on the DataFrame
        """

        last_update_ts = df.select(func.max(col(ts_field))).collect()[0][0]
        print(f"max value for {ts_field} is {last_update_ts}")

        return last_update_ts
