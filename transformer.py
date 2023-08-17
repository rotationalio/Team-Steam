import os

from pyensign.ensign import Ensign

# GLOBAL VARIABLES
PLAYER_QUERY = "http://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid="
ALL_GAMES_TOPIC = "all_games_json"

class SteamTransformer:
    """
    SteamTransformer queries the steam API and publishes events to Ensign.
    """

    def __init__(self, steam_key=None, ensign_key_path=None):
        """        
        steam_key : string, default: None
            You can put your API key for the Steam Developer API here. If you leave it
            blank, the publisher will attempt to read it from the STEAM_API_KEY environment 
            variable

        ensign_creds: string, default: None
            The file path to your ensign API key. If you leave it blank, the publisher 
            will attempt to read it from the ENSIGN_KEY_PATH environment variables.
        """
        if steam_key is None:
            self.steam_key = os.getenv("STEAM_API_KEY")
        else:
            self.steam_key = steam_key
        if self.steam_key is None:
            raise ValueError(
                "STEAM_API_KEY environment variable must be set")
        
        if ensign_key_path is None:
            self.ensign_key = os.getenv("ENSIGN_KEY_PATH")
        else:
            self.ensign_key = ensign_key_path
        if self.ensign_key is None:
            raise ValueError(
                "ENSIGN_KEY_PATH environment variable must be set")

        # Start a connection to the Ensign server.
        self.ensign = Ensign(cred_path=ensign_key_path)