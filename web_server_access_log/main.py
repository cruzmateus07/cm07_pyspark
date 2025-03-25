from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, current_timestamp, md5, regexp_extract
from python_utils.unzip import unzip
import os


## -------------------- Init -------------------- ##
spark = SparkSession.builder.getOrCreate()

cwd = os.getcwd()
zip_path = f"{cwd}/access_log.zip"
ziped_file = "access_log.txt"
extract_path = f"{cwd}/access_log_unzip/"
table_save_path = f"{cwd}/web_server_access_log/"

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

# Adding control columns to parsed_log_df for saving purposes
web_server_access_table = parsed_log_df \
    .withColumn("hash_id", md5(concat_ws("", col("ip_address"), col("timestamp")))) \
    .withColumn("meta$ingested_on", current_timestamp())

web_server_access_table.write.format("parquet").mode("overwrite").save(table_save_path)
