# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging
from collections import namedtuple
import cv2
import math
from datetime import datetime
import json
import tempfile
from azure.storage.blob import BlockBlobService
import sys
import os
sys.path.append(os.path.abspath('.'))
import MyClasses
import MyFunctions
import pytz
import pandas as pd

vidDets = namedtuple('VideoDetails',
                    ['blobDetails',
                    'timeToCutUTC',
                    'frameNumberList',
                    'sport',
                    'event',
                    'multipleVideoEvent',
                    'samplingProportion'])


def main(videoDetails: vidDets) -> list:
    ## Get blob details
    (blobDetails,timeToCutUTCStr,frameNumberList,
    sport,event,multipleVideoEvent,samplingProportion) = videoDetails
    blobOptions = json.loads(blobDetails)
    fileURL = blobOptions['fileUrl']
    container = blobOptions['container']
    fileName = blobOptions['blob']
    # if blobOptions['imagesAlreadyCreated'] is not None:
    #     imagesAlreadyCreated = int(float((blobOptions['imagesAlreadyCreated'])))
    # else:
    #     imagesAlreadyCreated = None
    if (blobOptions['imagesAlreadyCreated'] is None) or pd.isna(blobOptions['imagesAlreadyCreated']):
        imagesAlreadyCreated = None
    else:
        imagesAlreadyCreated = int(float((blobOptions['imagesAlreadyCreated'])))
    timeToCutUTC = datetime.strptime(timeToCutUTCStr,
                                    "%Y-%m-%d %H:%M:%S.%f")
    logging.info(f"fileURL: {fileURL}")
    logging.info(f"container: {container}")
    logging.info(f"fileName: {fileName}")
    logging.info(f"timeToCutUTCStr: {timeToCutUTCStr}")
    logging.info(f"imagesAlreadyCreated: {imagesAlreadyCreated}")
    ## Create BlockBlobService object
    logging.info("About to create BlockBlobService")
    block_blob_service = BlockBlobService(connection_string=os.environ['fsevideosConnectionString'])
    ## Get SAS file URL
    sasFileURL = MyFunctions.get_SAS_URL(
                        fileURL=fileURL,
                        block_blob_service=block_blob_service,
                        container=container
                        )
    ## Open the video
    vidcap = cv2.VideoCapture(sasFileURL)
    logging.info(f"VideoCapture object created for {fileURL}")
    success,image = vidcap.read()
    ## Get metadata
    fps = vidcap.get(cv2.CAP_PROP_FPS)
    fpsInt = int(round(fps,0))
    frameCount = int(vidcap.get(cv2.CAP_PROP_FRAME_COUNT))
    logging.info('Video metadata acquired')
    logging.info(f"(initial) frameCount: {str(frameCount)}")
    logging.info(f"(initial) FPS: {fps}")
    logging.info(f"(initial) int FPS: {fpsInt}")
    ## If frame count negative, download locally and try again
    if frameCount <= 0:
        logging.info("Frame count not greater than 0, so local download needed (ReturnFrameNumbers)")
        with tempfile.TemporaryDirectory() as dirpath:
            ## Get blob and save to local directory
            vidLocalPath = fr"{dirpath}\{fileName}"

            logging.info("BlockBlobService created")
            block_blob_service.get_blob_to_path(container_name=container,
                                                blob_name=fileName,
                                                file_path=vidLocalPath,
                                                max_connections=1)
            logging.info("Blob saved to path")
            with MyClasses.MyVideoCapture(vidLocalPath) as vc1:
                frameCount = int(vc1.get(cv2.CAP_PROP_FRAME_COUNT))
                fps = vc1.get(cv2.CAP_PROP_FPS)
                fpsInt = int(round(fps,0))

            logging.info(f"(new) frameCount: {str(frameCount)}")
            logging.info(f"(new) FPS: {fps}")
            logging.info(f"(new) int FPS: {fpsInt}")
    if timeToCutUTCStr != "2095-03-13 00:00:00.00000":
        utcTZ = pytz.timezone('UTC')
        etTZ = pytz.timezone('America/New_York')
        ## Work out when the recording starts based on the filename
        vidName = fileName.split("\\")[-1].replace(".mp4","")
        vidName1 = vidName[:vidName.index("-")]
        ## Get recording start and then assign it the US Eastern Time time zone
        recordingStart = datetime.strptime(f'{vidName1.split("_")[0]} {vidName1[-4:]}',
                                        "%Y%m%d %H%M")
        recordingStartET = etTZ.localize(recordingStart)
        ## Convert it to UTC
        recordingStartUTC = recordingStartET.astimezone(utcTZ).replace(tzinfo=None)
        ## Work out which frames to reject
        frameToCutFrom = int((timeToCutUTC - recordingStartUTC).seconds * fps)
    else:
        ## If last play is my 100th birthday, set a huge number that it'll never reach
        frameToCutFrom = 100000000000000
    logging.info("List of frame numbers about to be generated")
    ## Get number of frames wanted per second
    if samplingProportion is None:
        wantedFPS = 1
    else:
        wantedFPS = samplingProportion
    takeEveryN_requested = math.floor(fpsInt/wantedFPS)
    takeEveryN_1FPS = math.floor(fpsInt/1)
    logging.info(f"Taking 1 image for every {takeEveryN_requested} frames")
    ## Create list of frame numbers, under the assumption of 1 FPS
    listOfFrameNumbers_1FPS = [
        i
        for i in range(frameCount)
        if (i % takeEveryN_1FPS == 0) & (i <= frameToCutFrom)
        ]
    ## If video is shorter than a minute, return all the frames (@ 1 FPS)
    if len(listOfFrameNumbers_1FPS) <= 60:
        listOfFrameNumbersAndFrames = [
            (i,frame)
            for i,frame in enumerate(listOfFrameNumbers_1FPS,1)
        ]
    ## Otherwise, get the first min @ 1 FPS and the rest @ requested FPS
    else:
        ## Get first minute's frames @ 1 FPS
        firstMinute = [
            (i,frame)
            for i,frame in enumerate(listOfFrameNumbers_1FPS[:60],1)
        ]
        ## Create list of frame numbers to be JPEGed
        remainingMinutes = [
            (i,frame)
            for i,frame in enumerate(listOfFrameNumbers_1FPS[60:],61)
            if (frame % takeEveryN_requested == 0)
            ]
        ## Join them together
        listOfFrameNumbersAndFrames = firstMinute + remainingMinutes
    if imagesAlreadyCreated is not None:
        ## Some images have already been created, so cut them away
        logging.info(f"Initially, there were {len(listOfFrameNumbersAndFrames)} elements")
        listOfFrameNumbersAndFrames = listOfFrameNumbersAndFrames[imagesAlreadyCreated:]
        logging.info(f"Now there are {len(listOfFrameNumbersAndFrames)} elements")
    logging.info(f"listOfFrameNumbers created with {len(listOfFrameNumbersAndFrames)} elements")
    return json.dumps(listOfFrameNumbersAndFrames)