from pyspark.sql import SparkSession 
from pyspark.sql.functions import col, explode, lit
from pyspark.sql.types import ArrayType, BooleanType, IntegerType, StringType, StructField, StructType, TimestampType

import os


channel_id = 'UC5JY5JhsD4_GA2W6N8_Uaiw'
input_path = f'/Users/mateuscruz/Documents/Work/Projects/Warehouse/youtube/bronze/channel_{channel_id}'
output_path = f'/Users/mateuscruz/Documents/Work/Projects/Warehouse/youtube/silver/channel_{channel_id}'


spark = SparkSession.builder.getOrCreate()
channel_path = f"{input_path}/channel_data"
playlist_path = f"{input_path}/playlist_data"
playlist_item_path = f"{input_path}/playlist_items_data"
video_path = f"{input_path}/video_data"


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
).dropDuplicates()

channel_data_file_path = f"{output_path}/channel_data/"

channel_data.write.format("parquet").mode("overwrite").save(channel_data_file_path)


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
    col("item.snippet.thumbnails.default.url").alias("playlist_icon_picture_url"),
    col("item.contentDetails.itemCount").alias("playlist_video_count")
)

## Checks if directory exists, otherwise creates it
playlist_data_file_path = f"{output_path}/playlist_data/"
if not os.path.exists(playlist_data_file_path):
    os.makedirs(playlist_data_file_path)

playlist_data.write.format("parquet").mode("overwrite").save(playlist_data_file_path)


## --------------------- Playlist Items Data --------------------- ##
schema_playlist_items = StructType([
    StructField("items", ArrayType(StructType([
        StructField("contentDetails",StructType([
            StructField("videoId", StringType(), True)
        ]), True),
        StructField("snippet",StructType([
            StructField("playlistId",StringType(),True),
            StructField("publishedAt", TimestampType(), True),
            StructField("position", IntegerType(), True)
        ]), True)
    ])), True)
])

playlist_items_df = spark.read.schema(schema_playlist_items).json(f"{playlist_item_path}/*.json")

playlist_items_df_exploded = playlist_items_df.select(
    explode(col("items")).alias("item")
)

playlist_items_data = playlist_items_df_exploded.select(
    col("item.snippet.playlistId").alias("playlist_id"),
    col("item.contentDetails.videoId").alias("video_id"),
    col("item.snippet.publishedAt").alias("video_added_to_playlist_at_date"),
    col("item.snippet.position").alias("video_position")
)

## Checks if directory exists, otherwise creates it
playlist_items_data_file_path = f"{output_path}/playlist_items_data/"
if not os.path.exists(playlist_items_data_file_path):
    os.makedirs(playlist_items_data_file_path)

playlist_items_data.write.format("parquet").mode("overwrite").save(playlist_items_data_file_path)


## --------------------- Video Data --------------------- ##
schema_video = StructType([
    StructField("items", ArrayType(StructType([
        StructField("id",StringType(),True),
        StructField("snippet",StructType([
            StructField("title",StringType(),True),
            StructField("description",StringType(),True),
            StructField("publishedAt", TimestampType(), True),
            StructField("liveBroadcastContent", BooleanType(), True),
            StructField("thumbnails", StructType([
                StructField("default", StructType([
                    StructField("url", StringType(), True)
                ]), True),
            ]), True),
        ]), True),
        StructField("contentDetails",StructType([
            StructField("duration", StringType(), True),
            StructField("definition", StringType(), True),
            StructField("caption", StringType(), True),
            StructField("contentRating", StringType(), True)
        ]), True),
        StructField("statistics",StructType([
            StructField("viewCount", IntegerType(), True),
            StructField("likeCount", IntegerType(), True),
            StructField("favoriteCount", IntegerType(), True),
            StructField("commentCount", IntegerType(), True),
        ]), True)
    ])), True)
])

video_df = spark.read.schema(schema_video).json(f"{video_path}/*.json")

video_df_exploded = video_df.select(
    explode(col("items")).alias("item")
)

video_data = video_df_exploded.select(
    col("item.id").alias("video_id"),
    col("item.snippet.title").alias("video_name"),
    col("item.snippet.description").alias("video_description"),
    col("item.snippet.thumbnails.default.url").alias("video_icon_picture_url"),
    col("item.snippet.liveBroadcastContent").alias("is_live_broadcast"),
    col("item.contentDetails.duration").alias("video_duration"),
    col("item.contentDetails.definition").alias("video_definition"),
    col("item.contentDetails.caption").alias("is_caption_available"),
    col("item.contentDetails.contentRating").alias("video_content_rating"),
    col("item.statistics.viewCount").alias("video_view_count"),
    col("item.statistics.likeCount").alias("video_like_count"),
    col("item.statistics.favoriteCount").alias("video_favorite_count"),
    col("item.statistics.commentCount").alias("video_comment_count")
)

## Checks if directory exists, otherwise creates it
video_data_file_path = f"{output_path}/video_data/"
if not os.path.exists(video_data_file_path):
    os.makedirs(video_data_file_path)

video_data.write.format("parquet").mode("overwrite").save(video_data_file_path)
