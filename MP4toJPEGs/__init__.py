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
import json
from azure.storage.blob import BlockBlobService
import sys
import os
sys.path.append(os.path.abspath('.'))
import MyFunctions

vidDets = namedtuple('VideoDetails',
                        ['blobDetails',
                         'timeToCut'
                         'frameNumberList',
                         'sport',
                         'event'])


def main(videoDetails: vidDets) -> str:
    ## Get blob details
    blobDetails,timeToCut,frameNumberList,sport,event = videoDetails
    blobOptions = json.loads(blobDetails)
    container = blobOptions['container']
    fileURL = blobOptions['fileUrl']
    fileName = blobOptions['blob']
    # ## Get clean video name to be used as folder name (without ".mp4" on the end)
    # vidName = MyFunctions.cleanUpVidName(fileName.split("/")[-1])[:-4]
    ## Return the container name and connection string to insert images into
    containerOutput, connectionStringOutput = MyFunctions.getContainerAndConnString(
                                                        sport,
                                                        container
                                                        )
    ## Create BlockBlobService object to be used to upload blob to container
    block_blob_service = BlockBlobService(connection_string=connectionStringOutput)
    logging.info(f'BlockBlobService created for account "{block_blob_service.account_name}"')
    ## Create container (will do nothing if container already exists)
    block_blob_service.create_container(container_name=containerOutput)
    ## Open the video
    vidcap = cv2.VideoCapture(fileURL)
    ## Loop through the frame numbers
    for frameNumberName,frameNumber in enumerate(frameNumberList,1):
        ## Create path to save image to
        frameName = (5 - len(str(frameNumberName)))*"0" + str(frameNumberName)
        imagePath = fr"{event}\{frameName}.jpeg"
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
                byte_im = image2.tobytes() 
                ## Create the new blob
                block_blob_service.create_blob_from_bytes(container_name=containerOutput,
                                                            blob_name=imagePath,
                                                            blob=byte_im)

    return True