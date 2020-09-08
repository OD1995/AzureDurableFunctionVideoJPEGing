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

vidDets = namedtuple('VideoDetails',
                        ['blobDetails',
                         'timeToCut'
                         'frameNumberList',
                         'sport',
                         'event'])


def main(videoDetails: vidDets) -> list:
    ## Get blob details
    blobDetails,timeToCutStr,frameNumberList,sport,event = videoDetails
    blobOptions = json.loads(blobDetails)
    fileURL = blobOptions['fileUrl']
    container = blobOptions['container']
    fileName = blobOptions['blob']
    timeToCut = datetime.strptime(timeToCutStr,
                                    "%Y-%m-%d %H:%M:%S.%f")
    ## Open the video
    vidcap = cv2.VideoCapture(fileURL)
    logging.info(f"VideoCapture object created for {fileURL}")
    success,image = vidcap.read()
    ## Get metadata
    fps = vidcap.get(cv2.CAP_PROP_FPS)
    frameCount = int(vidcap.get(cv2.CAP_PROP_FRAME_COUNT))
    logging.info('Video metadata acquired')
    ## If frame count negative, download locally and try again
    if frameCount < 0:
        with tempfile.TemporaryDirectory() as dirpath:
            ## Get blob and save to local directory
            vidLocalPath = fr"{dirpath}\{fileName}"
            block_blob_service = BlockBlobService(connection_string=os.getenv("AzureWebJobsStorage"))
            block_blob_service.get_blob_to_path(container_name=container,
                                                blob_name=fileName,
                                                file_path=vidLocalPath)
            with MyClasses.MyVideoCapture(vidLocalPath) as vc1:
                frameCount = int(vc1.get(cv2.CAP_PROP_FRAME_COUNT))
    ## Get number of frames wanted per second
    wantedFPS = 1
    takeEveryN = math.floor(fps/wantedFPS)
    if timeToCutStr != "2095-03-13 00:00:00.00000":
        ## Work out when the recording starts based on the filename
        vidName = fileName.split("\\")[-1].replace(".mp4","")
        vidName1 = vidName[:vidName.index("-")]
        recordingStart = datetime.strptime(f'{vidName1.split("_")[0]} {vidName1[-4:]}',
                                            "%Y%m%d %H%M")
        ## Work out which frames to reject
        frameToCutFrom = int((timeToCut - recordingStart).seconds * fps)
    else:
        ## If last play is my 100th birthday, set a huge number that it'll never reach
        frameToCutFrom = 1000000000
    logging.info("List of frame numbers about to be generated")
    ## Create list of frame numbers to be JPEGed
    listOfFrameNumbers = [i
                            for i in range(frameCount)
                            if (i % takeEveryN == 0) & (i <= frameToCutFrom)]

    return listOfFrameNumbers