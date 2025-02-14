from pyspark.sql import SparkSession


channel_id = <<'channel_id'>>
input_path = <<'input_path'>>


spark = SparkSession.builder.getOrCreate()
channel_path = f"{input_path}channel_{channel_id}/channel_data"
playlist_path = f"{input_path}channel_{channel_id}/playlist_data"
playlist_item_path = f"{input_path}channel_{channel_id}/playlist_items_data"
video_path = f"{input_path}channel_{channel_id}/video_data_data"


## --------------------- Channel Data --------------------- ##
channel_df = spark.read.json(f"{channel_path}/*.json")

channel_data = channel_df.select(
    channel_df.items.snippet.getField("title")[0].alias("channel_name"),
    channel_df.items.snippet.getField("description")[0].alias("channel_description"),
    channel_df.items.snippet.getField("customUrl")[0].alias("channel_custom_url"),
    channel_df.items.snippet.getField("publishedAt")[0].alias("channel_launch_date"),
    channel_df.items.snippet.thumbnails.default.getField("url")[0].alias("channel_profile_picture_url"),
    channel_df.items.statistics.getField("viewCount")[0].alias("subscriberCount").alias("channel_subscribers_count"),
    channel_df.items.statistics.getField("videoCount")[0].alias("channel_video_count")
)


## --------------------- Playlist Data --------------------- ##
playlist_df = spark.read.json(f"{playlist_path}/*.json")

playlist_data = playlist_df.select(

)

## --------------------- Playlist Items Data --------------------- ##
playlist_items_df = spark.read.json(f"{playlist_item_path}/*.json")

playlist_items_data = playlist_items_df.select(

)

## --------------------- Video Data --------------------- ##
video_df = spark.read.json(f"{video_path}/*.json")

video_data = video_df.select(

)
