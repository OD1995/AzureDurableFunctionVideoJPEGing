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
from datetime import datetime
import os
import sys
sys.path.append(os.path.abspath('.'))
import MyFunctions
import azure.functions as func
import azure.durable_functions as df



def orchestrator_function(context: df.DurableOrchestrationContext):
    logging.info("Orchestrator started")

    ## Make sure the mp4 is in the right container
    _container_ = json.loads(context._input)['container']
    if _container_ not in [
        "us-office",
        "azure-video-to-image-import"
    ]:
        return f"Container = `{_container_}` so no processing needed"

    else:
        startUTCstr = datetime.strftime(context.current_utc_datetime,
                                        "%Y-%m-%d %H:%M:%S.%f")
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
            (videoID,sport,event,endpointID,
                multipleVideoEvent,samplingProportion) = abv[videoName[:-4]]
            for metric,value in [
                ("videoID",videoID),
                ("sport",sport),
                ("event",event),
                ("endpointID",endpointID),
                ("multipleVideoEvent",multipleVideoEvent),
                ("samplingProportion",samplingProportion)
            ]:
                logging.info(f"{metric}: {value}")
        except KeyError:
            videoID = None
            sport = None
            event = None
            endpointID = None
            multipleVideoEvent = None
            samplingProportion = None
            logging.info("Video not in AzureBlobVideos so relevant values assigned None")

        ## Make sure `videoName` has got a value, otherwise give it None
        try:
            videoName
        except NameError:
            videoName = None

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
                            'event',
                            'multipleVideoEvent',
                            'samplingProportion'])
        videoDetails = vidDets(blobDetails=context._input,
                                timeToCutUTC=timeToCutUTC,
                                frameNumberList=None,
                                sport=None,
                                event=None,
                                multipleVideoEvent=None,
                                samplingProportion=samplingProportion)
        logging.info("Initial videoDetails object created")
        listOfFrameNumbers = yield context.call_activity(
                                        name='ReturnFrameNumbers',
                                        input_=videoDetails)
        logging.info(f'List of {len(json.loads(listOfFrameNumbers))} generated')

        # Create images from list
        MP4toJPEGsoutput = yield context.call_activity(
                                        name='MP4toJPEGs',
                                        input_=vidDets(blobDetails=context._input,
                                                        timeToCutUTC=None,
                                                        frameNumberList=listOfFrameNumbers,
                                                        sport=sport,
                                                        event=event,
                                                        multipleVideoEvent=multipleVideoEvent,
                                                        samplingProportion=samplingProportion)
                                                )
        (imagesCreatedList,imagesCreatedCount,
            imageNames,
            outputContainer,outputBlobStorageAccount) = json.loads(MP4toJPEGsoutput)
        endUTCstr = datetime.strftime(context.current_utc_datetime,
                                        "%Y-%m-%d %H:%M:%S.%f")
        logging.info("Images generated!")


        ## If endpointID provided in `AzureBlobVideos`, add row to `ComputerVisionProccessingJobs` for each image
        if endpointID is not None:
            ## Create composite object to use
            QueueDetails = namedtuple('QueueDetails',
                                [
                                    'endpointID',
                                    'sport',
                                    'event',
                                    'blobDetails',
                                    'frameNumberList',
                                    'imagesCreatedList',
                                    'imageNames'
                                ])
            qpj_result = yield context.call_activity(
                name="QueueProcessingJobs",
                input_=QueueDetails(
                    endpointID=endpointID,
                    sport=sport,
                    event=event,
                    blobDetails=context._input,
                    frameNumberList=listOfFrameNumbers,
                    imagesCreatedList=imagesCreatedList,
                    imageNames=imageNames
                )
            )

        ## Add line to SQL - using another composite object
        UploadDetails = namedtuple('UploadDetails',
                            ['startUTC',
                            'endUTC',
                            'videoID',
                            'videoName',
                            'event',
                            'outputContainer',
                            'outputBlobStorageAccount',
                            'imagesCreatedCount'])
        wts_result = yield context.call_activity(
                                        name='WriteToSQL',
                                        input_=UploadDetails(
                                                        startUTC=startUTCstr,
                                                        endUTC=endUTCstr,
                                                        videoID=videoID,
                                                        videoName=videoName,
                                                        event=event,
                                                        outputContainer=outputContainer,
                                                        outputBlobStorageAccount=outputBlobStorageAccount,
                                                        imagesCreatedCount=imagesCreatedCount)
                                                )

        return f"{qpj_result} & {wts_result}" if endpointID is not None else wts_result
# logging.info("We're on line 83")
main = df.Orchestrator.create(orchestrator_function)