from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow

import os
import pickle


def youtube_auth():
    """
        Authenticates with Youtube API.
        Generates pickle file with users's access tokens.
        Requires external config on Google Cloud for Client OAuth.
    """

    # List of scopes using Youtube API
    SCOPES = ["https://www.googleapis.com/auth/youtube.force-ssl"]
    credentials = None

    api_service_name = 'youtube'
    api_version = 'v3'
    # Client Oauth downloaded from https://console.cloud.google.com/auth/clients
    client_secrets_file = 'credentials.json'

    # checks if token.pickle file exists before reading, otherwise proceeds to generate it for the first time
    if os.path.exists("token.pickle"):
        with open("token.pickle", "rb") as token:
            credentials = pickle.load(token)

    # if there are no valid credentials available let the user log in
    else:
        flow = InstalledAppFlow.from_client_secrets_file(
            client_secrets_file, SCOPES)
        credentials = flow.run_local_server(port=0)

        # saves credentials for next run as file token.pickle
        with open("token.pickle", "wb") as token:
            pickle.dump(credentials, token)

    return build(serviceName=api_service_name, version=api_version, developerKey=credentials)
