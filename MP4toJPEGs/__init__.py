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
from pathlib import Path
from datetime import datetime

vidDets = namedtuple('VideoDetails',
                        ['fileName',
                         'timeToCut'])


def main(videoDetails: vidDets) -> str:
    ## Get blob details
    blobOptions = (json.loads(videoDetails.blobDetails))
    container = blobOptions['container']
    blob =  blobOptions['blob']
    fileURL = blobOptions['fileUrl']
    ## Open the video
    vidcap = cv2.VideoCapture(fileURL)
    success,image = vidcap.read()
    ## Get metadata
    fps = vidcap.get(cv2.CAP_PROP_FPS)
    ## Get number of frames wanted per second
    wantedFPS = 1
    takeEveryN = math.floor(fps/wantedFPS)
    ## Work out when the recording starts based on the filename
    vidName = videoDetails.fileName.split("\\")[-1].replace(".mp4","")
    vidName1 = vidName[:vidName.index("-")]
    vidRoot = "\\".join(videoDetails.fileName.split("\\")[:-1])
    recordingStart = datetime.strptime(f'{vidName1.split("_")[0]} {vidName1[-4:]}',
                                        "%Y%m%d %H%M")
    ## Work out which frames to reject
    frameToCutFrom = int((videoDetails.timeToCut - recordingStart).seconds * fps)
    ## Create folder if one doesn't exist
    framesFolder = f"{vidRoot}\\{vidName1}"
    Path(framesFolder).mkdir(parents=True, exist_ok=True)
    count = 0
    frameNumber = 0
    while success:
        if count % takeEveryN == 0:
            ## Create path to save image to
            frameName = (5 - len(str(frameNumber)))*"0" + str(frameNumber)
            imagePath = fr"{framesFolder}\\{frameName}.jpg"
            ## Save image
            cv2.imwrite(imagePath,
                        image)
            frameNumber += 1
        success,image = vidcap.read()
        count += 1
        
        if count >= frameToCutFrom:
            success = False

    return framesFolder