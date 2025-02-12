import datetime
import json
import youtube_auth


def get_channel_data(youtube,channel_id,output_path,ct_ltz):
    """
        Function to get Youtube Channel data based on provided channel_id.
        Saves data to json file on output path of choice.
    """
    request = youtube.channels().list(
        ## Available channel list parts at: https://developers.google.com/youtube/v3/docs/channels/list
        part = "snippet,contentDetails,statistics",
        id = channel_id
    ).execute()

    with open(f"{output_path}channel/channel_data_{ct_ltz}", "w") as output:
        json.dump(request, output)


def get_playlist_data(youtube,channel_id,output_path,ct_ltz):
    """
        Function to get Youtube Playlist data based on provided channel_id.
        Saves data to json file on output path of choice.
    """
    request = youtube.playlists().list(
        ## Available playlist list parts at: https://developers.google.com/youtube/v3/docs/playlists/list
        part = "snippet,contentDetails",
        channelId = channel_id,
        maxResults=25
    ).execute()

    with open(f"{output_path}playlist/playlist_data_{ct_ltz}", "w") as output:
        json.dump(request, output)


if __name__ == "__main__":
    ## Input channel_id of choise
    channel_id = <<youtube_channel_id>>

    ## Output path for json file
    output_path = <<output_file_path>>

    ## Auth function call
    youtube = youtube_auth.youtube_auth()

    ## Get current timestamp
    ct_ltz = datetime.datetime.now()

    get_channel_data(youtube,channel_id,output_path,ct_ltz)
    get_playlist_data(youtube,channel_id,output_path,ct_ltz)
