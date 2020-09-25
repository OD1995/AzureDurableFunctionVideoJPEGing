# This function is not intended to be invoked directly. Instead it will be
# triggered by an HTTP starter function.
# Before running this sample, please:
# - create a Durable activity function (default name is "Hello")
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging
logging.info("`logging` has been imported")
import json
from collections import namedtuple
# import sys
import os
# logging.info("About to add to sys.path")
# sys.path.append(os.path.abspath('.'))
# logging.info("sys.path has been added to")
# import MyFunctions
# logging.info("`MyFunctions` has been imported")
from datetime import datetime
import pandas as pd
import pyodbc
import azure.functions as func
import azure.durable_functions as df
logging.info("About to start definition of `orchestrator_function`")

def getAzureBlobVideos2():
    logging.info("getAzureBlobVideos started")
    ## Get information used to create connection string
    username = 'matt.shepherd'
    # password = os.getenv("sqlPassword")
    password = "4rsenal!PG01"
    driver = '{ODBC Driver 17 for SQL Server}'
    # server = os.getenv("sqlServer")
    server = "fse-inf-live-uk.database.windows.net"
    database = 'AzureCognitive'
    table = 'AzureBlobVideos'
    ## Create connection string
    connectionString = f'DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password}'
    logging.info(f'Connection string created: {connectionString}')
    ## Create SQL query to use
    sqlQuery = f"SELECT * FROM {table}"
    with pyodbc.connect(connectionString) as conn:
        ## Get SQL table in pandas DataFrame
        df = pd.read_sql(sql=sqlQuery,
                            con=conn)
    logging.info(f"Dataframe with shape {df.shape} received")
    ## Dict - VideoName : (Sport,Event) 
    dfDict = {vn : (s,e)
                for vn,s,e in zip(df.VideoName,
                                    df.Sport,
                                    df.Event)}

    return dfDict

def cleanUpVidName(videoName0):
    """Clean up video name if '_HHMM-YYYY-mm-dd.mp4' is in the video name"""
    try:
        _ = datetime.strptime(videoName0[-15:-4],
                                "-%Y-%m-%d")
        return videoName0.replace(videoName0[-15:-4],"")
    except ValueError:
        return videoName0

def orchestrator_function(context: df.DurableOrchestrationContext):
    logging.info("Orchestrator started")
    
    ## Get AzureBlobVideos table from SQL, in dict form
    abv = getAzureBlobVideos2()
    logging.info(f"AzureBlobVideos table retrieved, rows: {len(abv)}")

    ## If the video name is in the dict, extract the information
    try:
        videoName0 = json.loads(context._input)['blob']
        ## If last 11 characters (excluding '.mp4') follow '-YYYY-MM-DD'
        ##    then remove them
        videoName = cleanUpVidName(videoName0)
        ## Get relevant sport and event name for the video (excluding '.mp4')
        sport,event = abv[videoName[:-4]]
        logging.info("sport and event retrieved")
    except KeyError:
        sport = None
        event = None
        logging.info("Video not in AzureBlobVideos so sport and event both None")

    if sport == 'baseball':
        ## Get time to cut from, using MLB API
        timeToCut = yield context.call_activity(name='CallAPI',
                                                input_=context._input)
        logging.info('timeToCut acquired from API')
    else:
        ## Make timeToCut a time far in the future (my 100th birthday)
        timeToCut = "2095-03-13 00:00:00.00000"
        logging.info("Not baseball, so distant timeToCut provided")

    ## Get list of frame numbers to convert to JPEGs, ending at `timeToCut`
    ##    Use composite object
    ##     - https://docs.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-orchestrations?tabs=python#passing-multiple-parameters
    vidDets = namedtuple('VideoDetails',
                         ['blobDetails',
                          'timeToCut',
                          'frameNumberList',
                          'sport',
                          'event'])
    videoDetails = vidDets(blobDetails=context._input,
                            timeToCut=timeToCut,
                            frameNumberList=None,
                            sport=None,
                            event=None)
    logging.info("Initial videoDetails object created")
    listOfFrameNumbers = yield context.call_activity(
                                    name='ReturnFrameNumbers',
                                    input_=videoDetails)
    logging.info(f'List of {len(json.loads(listOfFrameNumbers))} generated')

    ## Create images from list
    values = yield context.call_activity(
                                    name='MP4toJPEGs',
                                    input_=vidDets(blobDetails=context._input,
                                                    timeToCut=None,
                                                    frameNumberList=listOfFrameNumbers,
                                                    sport=sport,
                                                    event=event)
                                            )

    return values
logging.info("We're on line 83")
main = df.Orchestrator.create(orchestrator_function)