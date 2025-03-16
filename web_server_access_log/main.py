from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract
from python_utils.unzip import unzip
import os


## -------------------- Init -------------------- ##
spark = SparkSession.builder.getOrCreate()

cwd = os.getcwd()
zip_path = f"{cwd}/access_log.zip"
ziped_file = "access_log.txt"
extract_path = f"{cwd}/access_log_unzip/"

unzip(zip_path, ziped_file, extract_path)

# Load text file into spark DataFrame
raw_log_df = spark.read.text(extract_path)

# Given that the log file follows the Web Server Access Log,
# the following regexp pattern can be used to parse the file
log_pattern = r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(.*?)" (\d{3}) (\d+|-)'


## -------------------- DataFrame Manipulation -------------------- ##
# Extract fields using regex
parsed_log_df = raw_log_df.select(
    regexp_extract('value', log_pattern, 1).alias('ip_address'),
    regexp_extract('value', log_pattern, 2).alias('client_identity'),
    regexp_extract('value', log_pattern, 3).alias('user_id'),
    regexp_extract('value', log_pattern, 4).alias('timestamp'),
    regexp_extract('value', log_pattern, 5).alias('request'),
    regexp_extract('value', log_pattern, 6).alias('status_code'),
    regexp_extract('value', log_pattern, 7).alias('response_size')
)
