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
from azure.storage.blob import BlockBlobService
import os

vidDets = namedtuple('VideoDetails',
                        ['blobDetails',
                        'timeToCut',
                        'frameNumber',
                        'outputBlob'])


def main(videoDetails: vidDets) -> str:
    ## Get blob details
    blobOptions = (json.loads(videoDetails.blobDetails))
    container = blobOptions['container']
    blob =  blobOptions['blob']
    fileURL = blobOptions['fileUrl']
    frameNumber = blobOptions.frameNumber
    ## Create BlockBlobService object to be used to upload blob to container
    block_blob_service = BlockBlobService(connection_string=os.getenv("AzureWebJobsStorage"))
    # logging.info(f'BlockBlobService created for account "{block_blob_service.account_name}"')
    ## Create path to save image to
    frameName = (5 - len(str(frameNumber)))*"0" + str(frameNumber)
    imagePath = fr"{framesFolder}\\{frameName}.jpeg"
    ## Open the video
    vidcap = cv2.VideoCapture(fileURL)
    ## Set the video to the correct frame
    vidcap.set(cv2.CAP_PROP_POS_FRAMES,
                frameNumber)
    ## Create the image
    success,image = vidcap.read()
    if success:
        ## Encode image
        success2, image2 = cv2.imencode(".jpeg", image)
        if success2:
            ## Convert image2 (numpy.ndarray) to bytes
            byte_im = im_buf_arr.tobytes() 
            ## Create the new blob
            block_blob_service.create_blob_from_bytes(container_name=container,
                                                        blob_name=imagePath',
                                                        blob=byte_im)

    return True