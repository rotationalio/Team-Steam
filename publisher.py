import os
import json
import asyncio
import warnings

import requests
from datetime import datetime
from pyensign.events import Event
from pyensign.ensign import Ensign

# TODO Python>3.10 needs to ignore DeprecationWarning: There is no current event loop
warnings.filterwarnings("ignore")

# GLOBAL VARIABLES
GAME_LIST_ENDPOINT = "https://api.steampowered.com/ISteamApps/GetAppList/v2/"
ALL_GAMES_TOPIC = "all_games_json"



class SteamPublisher:
    """
    SteamPublisher queries the steam API and publishes events to Ensign.
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

    async def print_ack(self, ack):
        """
        Enable the Ensign server to notify the Publisher the event has been acknowledged
        """
        ts = datetime.fromtimestamp(
            ack.committed.seconds + ack.committed.nanos / 1e9)
        print(f"Event committed at {ts}")

    async def print_nack(self, nack):
        """
        Enable the Ensign server to notify the Publisher if an event has NOT been
        acknowledged
        """
        print(f"Event was not committed with error {nack.code}: {nack.error}")

    def get_game_list(self):
        """
        Wrapper for an intermediate call to get the latest game list

        Returns a list of dictionaries of the form:
            {
              "name": "name_of_game",
              "appid": "steam_identifier
            }
        """
        game_info = requests.get(GAME_LIST_ENDPOINT).json()
        game_list = game_info.get("applist", None)
        if game_list is None:
            raise Exception("missing game list in Steam API response")
        all_games = game_list.get("apps", None)
        if all_games is None:
            raise Exception("missing app names in Steam API response")

        return all_games

    # TODO: implement check_index, the current behaviour will be to 
    # go through all games GAME_LIST_ENDPOINT returns.
    def check_index() -> int:
        """
        Retrieve from the all_games_json topic the highest game index  
        processed so far.
        """
        return 0

    def create_event(self, game_id, game_name):
        data = {
            "game": game_name,
            "id": game_id
        }

        return Event(json.dumps(data).encode("utf-8"), mimetype="application/json")

    async def recv_and_publish(self):
        """
        ping the Steam API to get any new games from the GAME_LIST_ENDPOINT endpoint and
        publish events for new games.

        Publish report data to the ALL_GAMES_TOPIC topic.
        """
        if not await self.ensign.topic_exists(ALL_GAMES_TOPIC):
            raise Exception(f"topic {ALL_GAMES_TOPIC} does not exist")

        while True:
            all_games = self.get_game_list()

            index = self.check_index()
            if len(all_games) > index:
                raise IndexError(f"ensign topic index {index} out of range of game list")

            # Retrieve the player count for the current game/appid
            for game in all_games[index:]:
                game_name = game.get("name", None)
                game_id = game.get("appid", None)
                
                if game_id is None or game_name is None:
                    continue

                # Convert the response to an event and publish it
                event = self.create_event(game_id, game_name)
                await self.ensign.publish(
                    self.topic,
                    event,
                    on_ack=self.print_ack,
                    on_nack=self.print_nack
                )

            # sleep for a bit before we ping the API again
            await asyncio.sleep(self.interval)

    def run(self):
        """
        Run the steam publisher forever.
        """
        asyncio.run(self.recv_and_publish())


if __name__ == "__main__":
    publisher = SteamPublisher(ensign_key_path="secret/publish_creds.json")
    publisher.run()
