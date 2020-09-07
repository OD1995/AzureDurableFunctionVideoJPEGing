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
from . import MyFunctions
import azure.functions as func
import azure.durable_functions as df


def orchestrator_function(context: df.DurableOrchestrationContext):

    ## Get AzureBlobVideos table from SQL, in dict form
    abv = MyFunctions.getAzureBlobVideos()

    ## If the video name is in the dict, extract the information
    try:
        videoName = json.loads(context._input)['blob']
        ## MAKE ADJUSTMENTS FOR SNAPSTREAM ADDING DATE/TIME TO END

        sport,event = abv[videoName]
    except KeyError:
        sport = None
        event = None

    if sport == 'baseball':
        ## Get time to cut from, using MLB API
        timeToCut = yield context.call_activity(name='CallAPI',
                                                input_=context._input)
        logging.info('timeToCut acquired from API')
    else:
        ## Make timeToCut a time far in the future
        timeToCut = "2095-13-03 00:00:00.00000"
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
    logging.info('vidDets created')
    videoDetails = vidDets(blobDetails=context._input,
                            timeToCut=timeToCut,
                            frameNumberList=None,
                            sport=None,
                            event=None)
    logging.info('videoDetails created')
    listOfFrameNumbers = yield context.call_activity(
                                    name='ReturnFrameNumbers',
                                    input_=videoDetails)
    logging.info(f'List of {len(listOfFrameNumbers)} generated')

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

main = df.Orchestrator.create(orchestrator_function)