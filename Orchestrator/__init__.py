# This function is not intended to be invoked directly. Instead it will be
# triggered by an HTTP starter function.
# Before running this sample, please:
# - create a Durable activity function (default name is "Hello")
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging
import json
from collections import namedtuple
import os
import sys
sys.path.append(os.path.abspath('.'))
import MyFunctions
import azure.functions as func
import azure.durable_functions as df



def orchestrator_function(context: df.DurableOrchestrationContext):
    logging.info("Orchestrator started")
    
    ## Get AzureBlobVideos table from SQL, in dict form
    abv = MyFunctions.getAzureBlobVideos2()
    logging.info(f"AzureBlobVideos table retrieved, rows: {len(abv)}")

    ## If the video name is in the dict, extract the information
    try:
        videoName0 = json.loads(context._input)['blob']
        ## If last 11 characters (excluding '.mp4') follow '-YYYY-MM-DD'
        ##    then remove them
        videoName = MyFunctions.cleanUpVidName(videoName0)
        ## Get relevant sport and event name for the video (excluding '.mp4')
        sport,event = abv[videoName[:-4]]
        logging.info(f"sport ({sport}) and event ({event}) retrieved")
    except KeyError:
        sport = None
        event = None
        logging.info("Video not in AzureBlobVideos so sport and event both None")

    if sport == 'baseball':
        ## Get time to cut from, using MLB API
        timeToCutUTC = yield context.call_activity(name='CallAPI',
                                                    input_=context._input)
        logging.info('timeToCutUTC acquired from API')
    else:
        ## Make timeToCutUTC a time far in the future (my 100th birthday)
        timeToCutUTC = "2095-03-13 00:00:00.00000"
        logging.info("Not baseball, so distant timeToCutUTC provided")

    ## Get list of frame numbers to convert to JPEGs, ending at `timeToCutUTC`
    ##    Use composite object
    ##     - https://docs.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-orchestrations?tabs=python#passing-multiple-parameters
    vidDets = namedtuple('VideoDetails',
                         ['blobDetails',
                          'timeToCutUTC',
                          'frameNumberList',
                          'sport',
                          'event'])
    videoDetails = vidDets(blobDetails=context._input,
                            timeToCutUTC=timeToCutUTC,
                            frameNumberList=None,
                            sport=None,
                            event=None)
    logging.info("Initial videoDetails object created")
    listOfFrameNumbers = yield context.call_activity(
                                    name='ReturnFrameNumbers',
                                    input_=videoDetails)
    logging.info(f'List of {len(json.loads(listOfFrameNumbers))} generated')

    # Create images from list
    values = yield context.call_activity(
                                    name='MP4toJPEGs',
                                    input_=vidDets(blobDetails=context._input,
                                                    timeToCutUTC=None,
                                                    frameNumberList=listOfFrameNumbers,
                                                    sport=sport,
                                                    event=event)
                                            )
    logging.info("Images generated!")
    return values
logging.info("We're on line 83")
main = df.Orchestrator.create(orchestrator_function)