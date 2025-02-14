from pyspark.sql import SparkSession 
from pyspark.sql.functions import col, explode, lit
from pyspark.sql.types import ArrayType, IntegerType, StringType, StructField, StructType, TimestampType


channel_id = <<'channel_id'>>
input_path = <<'input_path'>>


spark = SparkSession.builder.getOrCreate()
channel_path = f"{input_path}channel_{channel_id}/channel_data"
playlist_path = f"{input_path}channel_{channel_id}/playlist_data"
playlist_item_path = f"{input_path}channel_{channel_id}/playlist_items_data"
video_path = f"{input_path}channel_{channel_id}/video_data"


## --------------------- Channel Data --------------------- ##
channel_schema = StructType([
    StructField("items", ArrayType(StructType([
        StructField("snippet",StructType([
            StructField("title", StringType(), True),
            StructField("description", StringType(), True),
            StructField("customUrl", StringType(), True),
            StructField("publishedAt", StringType(), True),
            StructField("thumbnails", StructType([
                StructField("default", StructType([
                    StructField("url", StringType(), True)
                ]), True),
            ]), True),
        ]), True),
        StructField("statistics",StructType([
            StructField("viewCount", StringType(), True),
            StructField("subscriberCount", StringType(), True),
            StructField("videoCount", StringType(), True)
        ]), True)
    ])), True)
])

channel_df = spark.read.schema(channel_schema).json(f"{channel_path}/*.json")

channel_df_exploded = channel_df.select(
    explode(col("items")).alias("item")
)

channel_data = channel_df_exploded.select(
    lit(channel_id).alias("channel_id"),
    col("item.snippet.title").alias("channel_name"),
    col("item.snippet.description").alias("channel_description"),
    col("item.snippet.customUrl").alias("channel_custom_url"),
    col("item.snippet.publishedAt").alias("channel_creation_date"),
    col("item.snippet.thumbnails.default.url").alias("channel_profile_picture_url"),
    col("item.statistics.viewCount").alias("channel_view_count"),
    col("item.statistics.subscriberCount").alias("channel_subscribers_count"),
    col("item.statistics.videoCount").alias("channel_video_count")
).dropDuplicates().show()


## --------------------- Playlist Data --------------------- ##
schema_playlist = StructType([
    StructField("items", ArrayType(StructType([
        StructField("id",StringType(),True),
        StructField("snippet",StructType([
            StructField("title", StringType(), True),
            StructField("description", StringType(), True),
            StructField("publishedAt", TimestampType(), True),
            StructField("thumbnails", StructType([
                StructField("default", StructType([
                    StructField("url", StringType(), True)
                ]), True),
            ]), True),
        ]), True),
        StructField("contentDetails",StructType([
            StructField("itemCount", IntegerType(), True),
        ]), True)
    ])), True)
])

playlist_df = spark.read.schema(schema_playlist).json(f"{playlist_path}/*.json")

playlist_df_exploded = playlist_df.select(
    explode(col("items")).alias("item")
)

playlist_data = playlist_df_exploded.select(
    lit(channel_id).alias("channel_id"),
    col("item.id").alias("playlist_id"), 
    col("item.snippet.title").alias("playlist_title"),
    col("item.snippet.description").alias("playlist_description"),
    col("item.snippet.publishedAt").alias("playlist_created_at_date"),
    col("item.snippet.thumbnails.default.url").alias("playlist_profile_picture_url"),
    col("item.contentDetails.itemCount").alias("playlist_video_count")
).show()


## --------------------- Playlist Items Data --------------------- ##
schema_playlist_items = 'bla'

playlist_items_df = spark.read.schema(schema_playlist_items).json(f"{playlist_item_path}/*.json")

playlist_items_df_exploded = playlist_items_df.select(
    explode(col("items")).alias("item")
)

playlist_items_data = playlist_items_df_exploded.select(
)

## --------------------- Video Data --------------------- ##
schema_video = 'bla'

video_df = spark.read.schema(schema_video).json(f"{video_path}/*.json")

video_df_exploded = video_df.select(
    explode(col("items")).alias("item")
)

video_data = video_df_exploded.select(
)

