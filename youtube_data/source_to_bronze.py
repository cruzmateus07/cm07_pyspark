import datetime
import json
import os
import youtube_auth


def get_channel_data(youtube,channel_id,output_path,ct_ltz):
    """
        Function to get all data associated to Youtube Channel.
        Channel, Playlists, Playlists Items, Videos
    """


    ## --------------------- Channel Data  --------------------- ##
    channel_request = youtube.channels().list(
        ## Available channel list parts at: https://developers.google.com/youtube/v3/docs/channels/list
        part = "snippet,contentDetails,statistics",
        id = channel_id
    ).execute()

    ## Checks if directory exists, otherwise creates it
    channel_json_file_path = f"{output_path}channel_{channel_id}/channel_data/"
    if not os.path.exists(channel_json_file_path):
        os.makedirs(channel_json_file_path)
    
    ## Saves Channel Data in Json Format Files
    with open(f"{channel_json_file_path}{ct_ltz}", "w") as output:
        json.dump(channel_request, output)


    ## --------------------- Playlist Data  --------------------- ##
    playlist_request = youtube.playlists().list(
        ## Available playlist list parts at: https://developers.google.com/youtube/v3/docs/playlists/list
        part = "snippet,contentDetails",
        channelId = channel_id,
        maxResults=25
    ).execute()

    ## Checks if directory exists, otherwise creates it
    playlist_json_file_path = f"{output_path}channel_{channel_id}/playlist_data/"
    if not os.path.exists(playlist_json_file_path):
        os.makedirs(playlist_json_file_path)

    with open(f"{playlist_json_file_path}{ct_ltz}", "w") as output:
        json.dump(playlist_request, output)


    ## --------------------- Playlist Items Data  --------------------- ##
    ## Checks if directory exists, otherwise creates it
    playlist_items_json_file_path = f"{output_path}channel_{channel_id}/playlist_items_data/"
    if not os.path.exists(playlist_items_json_file_path):
        os.makedirs(playlist_items_json_file_path)

    items = playlist_request.get("items")

    playlist_ids = []

    for item in items:
        playlist_id = item["id"]

        playlist_item_request = youtube.playlistItems().list(
            ## Available playlist list parts at: https://developers.google.com/youtube/v3/docs/playlistItems/list
            part = "snippet,contentDetails",
            playlistId = playlist_id,
            maxResults=25
        ).execute()

        with open(f"{playlist_items_json_file_path}playlist_{playlist_id}_items_{ct_ltz}", "w") as output:
            json.dump(playlist_item_request, output)


        ## --------------------- Video Data  --------------------- ##
        ## Checks if directory exists, otherwise creates it
        video_json_file_path = f"{output_path}channel_{channel_id}/video_data/"
        if not os.path.exists(video_json_file_path):
            os.makedirs(video_json_file_path)

        items = playlist_item_request.get("items")

        video_ids = []

        for item in items:
            contents_details = item.get("contentDetails")
            video_id = contents_details.get("videoId")
            video_ids += [video_id]

        for id in video_ids:
            video_request = youtube.videos().list(
                ## Available playlist list parts at: https://developers.google.com/youtube/v3/docs/playlistItems/list
                part = "snippet,contentDetails",
                id = id,
                maxResults=25
            ).execute()

            with open(f"{video_json_file_path}video_{id}_{ct_ltz}", "w") as output:
                json.dump(video_request, output)


if __name__ == "__main__":
    ## Input channel_id of choise
    channel_id = 'UC5JY5JhsD4_GA2W6N8_Uaiw'

    ## Output path for json file
    output_path = '/Users/mateuscruz/Documents/Work/Projects/Warehouse/youtube/bronze/'

    ## Auth function call
    youtube = youtube_auth.youtube_auth()

    ## Get current timestamp
    ct_ltz = datetime.datetime.now()

    get_channel_data(youtube,channel_id,output_path,ct_ltz)
