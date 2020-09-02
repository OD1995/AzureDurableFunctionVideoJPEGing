# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging
import requests
from datetime import datetime, timedelta
import dateutil.parser
from pytz import timezone
import json

nameLookup = {
                'ARI' : 'Diamondbacks'
                ,'ATL' : 'Braves'
                ,'BAL' : 'Orioles'
                ,'BOS' : 'Red Sox'
                ,'CHC' : 'Cubs'
                ,'CIN' : 'Reds'
                ,'COL' : 'Rockies'
                ,'CLE' : 'Indians'
                ,'CWS' : 'White Sox'
                ,'DET' : 'Tigers'
                ,'HOU' : 'Astros'
                ,'KC' : 'Royals'
                ,'LAA' : 'Angels'
                ,'LAD' : 'Dodgers'
                ,'MIA' : 'Marlins'
                ,'MIL' : 'Brewers'
                ,'MIN' : 'Twins'
                ,'NYM' : 'Mets'
                ,'NYY' : 'Yankees'
                ,'OAK' : 'Athletics'
                ,'PHI' : 'Phillies'
                ,'PIT' : 'Pirates'
                ,'SD' : 'Padres'
                ,'SF' : 'Giants'
                ,'SEA' : 'Mariners'
                ,'STL' : 'Cardinals'
                ,'TB' : 'Rays'
                ,'TEX' : 'Rangers'
                ,'TOR' : 'Blue Jays'
                ,'WSH' : 'Nationals'
                }


def main(options: str) -> int:
    global nameLookup
    ## Get blob details
    blobOptions = (json.loads(options))
    blob =  blobOptions['blob']
    ## Get details from video name
    vidName = blob.split('/')[-1].replace(".mp4","")
    vidName1 = vidName[:vidName.index("-")]
    awayTeam,homeTeam = vidName1.split("_")[-2].split("@")
    recordingStart = datetime.strptime(f'{vidName1.split("_")[0]} {vidName1[-4:]}',
                                        "%Y%m%d %H%M")
    ## # MLB API
    ## Get API friendly date
    apiDate = datetime.strftime(recordingStart.date(),
                                "%m/%d/%Y")
    ## Call MLB API to get gameID
    schedResp = requests.get("https://statsapi.mlb.com/api/v1/schedule",
                            params={'sportId' : 1,
                                    'date' : apiDate})
    schedJS = schedResp.json()
    logging.info(f"MLB API called for {apiDate}")
    listOfGames = schedJS['dates'][0]['games']
    ## Create cleaner and more filterable list of dicts
    betterListOfGames = []
    for log in listOfGames:
        tba = {}
        tba['g'] = log['gamePk']
        tba['h'] = log['teams']['home']['team']['name']
        tba['a'] = log['teams']['away']['team']['name']
        betterListOfGames.append(tba)
    ## Get the game based on the video
    gameID = [x
            for x in betterListOfGames
            if (nameLookup[homeTeam] in x['h']) &
                (nameLookup[awayTeam] in x['a'])][0]['g']
    ## Call MLB API to get game end
    gameResp = requests.get(f"https://statsapi.mlb.com/api/v1.1/game/{gameID}/feed/live")
    gameJS = gameResp.json()
    ## Get all plays not containing "status change"
    allPlays = [x
                for x in gameJS['liveData']['plays']['allPlays']
                if "status change" not in x['playEvents'][0]['details']['description'].lower()]
    ## Get last play end time
    lastPlay = allPlays[-1]
    lastPlayEndString = lastPlay['about']["endTime"]
    lastPlayEnd = dateutil.parser.parse(
                        lastPlayEndString
                                    ).astimezone(
                                            timezone(
                                                'America/Chicago'
                                                    )
                                                ).replace(tzinfo=None)
    ## Work out which frames to reject
    timeToCut = lastPlayEnd + timedelta(hours=1)
    logging.info(f"Time to cut ({timeToCut}) retrieved")
    return datetime.strftime(timeToCut,
                                "%Y-%m-%d %H:%M:%S.%f")