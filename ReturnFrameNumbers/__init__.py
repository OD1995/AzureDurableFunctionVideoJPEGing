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

vidDets = namedtuple('VideoDetails',
                        ['blobDetails',
                         'timeToCut'
                         'frameNumber',
                         'outputBlob'])


def main(videoDetails: vidDets) -> list:
    ## Get blob details
    blobOptions = (json.loads(videoDetails.blobDetails))
    fileURL = blobOptions['fileUrl']
    fileName = blobOptions['blob']
    ## Open the video
    vidcap = cv2.VideoCapture(fileURL)
    success,image = vidcap.read()
    ## Get metadata
    fps = vidcap.get(cv2.CAP_PROP_FPS)
    frameCount = int(vidcap.get(cv2.CAP_PROP_FRAME_COUNT))
    ## Get number of frames wanted per second
    wantedFPS = 1
    takeEveryN = math.floor(fps/wantedFPS)
    ## Work out when the recording starts based on the filename
    vidName = fileName.split("\\")[-1].replace(".mp4","")
    vidName1 = vidName[:vidName.index("-")]
    vidRoot = "\\".join(fileName.split("\\")[:-1])
    recordingStart = datetime.strptime(f'{vidName1.split("_")[0]} {vidName1[-4:]}',
                                        "%Y%m%d %H%M")
    ## Work out which frames to reject
    frameToCutFrom = int((videoDetails.timeToCut - recordingStart).seconds * fps)
    ## Create list of frame numbers to be JPEGed
    listOfFrameNumbers = [i
                            for i in range(frameCount)
                            if (i % takeEveryN == 0) & (i <= frameToCutFrom)]

    return listOfFrameNumbers