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

vidDets = namedtuple('VideoDetails',
                        ['blobDetails',
                         'timeToCutUTC',
                         'frameNumberList',
                         'sport',
                         'event'])


def main(videoDetails: vidDets) -> list:
    ## Get blob details
    blobDetails,timeToCutUTCStr,frameNumberList,sport,event = videoDetails
    blobOptions = json.loads(blobDetails)
    fileURL = blobOptions['fileUrl']
    container = blobOptions['container']
    fileName = blobOptions['blob']
    timeToCutUTC = datetime.strptime(timeToCutUTCStr,
                                    "%Y-%m-%d %H:%M:%S.%f")
    logging.info(f"fileURL: {fileURL}")
    logging.info(f"container: {container}")
    logging.info(f"fileName: {fileName}")
    logging.info(f"timeToCutUTCStr: {timeToCutUTCStr}")
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
    ## Get number of frames wanted per second
    wantedFPS = 1
    takeEveryN = math.floor(fpsInt/wantedFPS)
    logging.info(f"Taking 1 image for every {takeEveryN} frames")
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
    ## Create list of frame numbers to be JPEGed
    listOfFrameNumbers = [i
                            for i in range(frameCount)
                            if (i % takeEveryN == 0) & (i <= frameToCutFrom)]
    logging.info(f"listOfFrameNumbers created with {len(listOfFrameNumbers)} elements")
    return json.dumps(listOfFrameNumbers)