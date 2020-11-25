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
import os
import sys
sys.path.append(os.path.abspath('.'))
import MyClasses
import MyFunctions
import tempfile

vidDets = namedtuple('VideoDetails',
                        ['blobDetails',
                         'timeToCutUTC',
                         'frameNumberList',
                         'sport',
                         'event'])


def main(videoDetails: vidDets):
    ## Get blob details
    blobDetails,timeToCutUTC,frameNumberList0,sport,event = videoDetails
    blobOptions = json.loads(blobDetails)
    container = blobOptions['container']
    fileURL = blobOptions['fileUrl']
    fileName = blobOptions['blob']
    frameNumberList = json.loads(frameNumberList0)
    logging.info(f"frameNumberList (type: {type(frameNumberList)}, length: {len(frameNumberList)}) received")
    # ## Get clean video name to be used as folder name (without ".mp4" on the end)
    # vidName = MyFunctions.cleanUpVidName(fileName.split("/")[-1])[:-4]
    ## Return the container name and connection string to insert images into
    containerOutput, connectionStringOutput,bsaOutput = MyFunctions.getContainerAndConnString(
                                                        sport,
                                                        container
                                                        )
    logging.info(f"containerOutput: {containerOutput}")
    logging.info(f"connectionStringOutput: {connectionStringOutput}")
    ## Set the file name to be used
    if event is not None:
        fileNameFolder = event
    else:
        ## Blob name without ".mp4"
        fileNameFolder = fileName.split("/")[-1][:-4]
    logging.info(f"fileNameFolder: {fileNameFolder}")
    ## Create BlockBlobService object to be used to get blob from container
    block_blob_serviceINPUT = BlockBlobService(connection_string=os.getenv("fsevideosConnectionString"))
    ## Create BlockBlobService object to be used to upload blob to container
    block_blob_serviceOUTPUT = BlockBlobService(connection_string=connectionStringOutput)
    logging.info(f'BlockBlobService created for account "{block_blob_serviceOUTPUT.account_name}"')
    ## Get names of all containers in the blob storage account
    containerNames = [x.name
                        for x in block_blob_serviceOUTPUT.list_containers()]
    logging.info(f"List of containerNames received, length: {len(containerNames)}")
    ## Create container (will do nothing if container already exists)
    if containerOutput not in containerNames:
        logging.info(f"Container '{containerOutput}' doesn't exist")
        _ = block_blob_serviceOUTPUT.create_container(container_name=containerOutput,
                                                            fail_on_exist=False)
        logging.info(f"Container '{containerOutput}' didn't exist, now has been created")
    else:
        logging.info(f"Container '{containerOutput}' exists already'")
    ## Get SAS file URL
    sasFileURL = MyFunctions.get_SAS_URL(
                        fileURL=fileURL,
                        block_blob_service=block_blob_serviceINPUT,
                        container=container
                        )
    logging.info(f"sasFileURL: {sasFileURL}")
    ## Open the video
    vidcap = cv2.VideoCapture(sasFileURL)
    logging.info("VideoCapture object created")
    success,image = vidcap.read()
    ## Get metadata
    fps = vidcap.get(cv2.CAP_PROP_FPS)
    frameCount = int(vidcap.get(cv2.CAP_PROP_FRAME_COUNT))
    logging.info('Video metadata acquired')
    logging.info(f"frameCount: {str(frameCount)}")
    logging.info(f"FPS: {fps}")
    ## Create variable to keep track of number of images generated
    imagesCreated = []
    ## If frame count negative, download locally and try again
    if frameCount <= 0:
        logging.info("Frame count not greater than 0, so local download needed (MP4toJPEGs)")
        with tempfile.TemporaryDirectory() as dirpath:
            ## Get blob and save to local directory
            vidLocalPath = fr"{dirpath}\{fileName}"
            # logging.info("About to get connection string")
            # logging.info(f"CS: {os.environ['fsevideosConnectionString']}")
            logging.info(f"About to save blob to path: '{vidLocalPath}'")
            block_blob_serviceINPUT.get_blob_to_path(container_name=container,
                                                blob_name=fileName,
                                                file_path=vidLocalPath,
                                                max_connections=1)
            logging.info("Blob saved to path")
            with MyClasses.MyVideoCapture(vidLocalPath) as vc1:
                for frameNumberName,frameNumber in enumerate(frameNumberList,1):
                    ## Create blobs
                    imageCreated = MyFunctions.createBlobs(
                                vc1,
                                frameNumber,
                                frameNumberName,
                                fileNameFolder,
                                block_blob_serviceOUTPUT,
                                containerOutput
                                )
                    imagesCreated += imageCreated
    else:
        ## Loop through the frame numbers
        for frameNumberName,frameNumber in enumerate(frameNumberList,1):
            ## Create blobs
            imageCreated = MyFunctions.createBlobs(
                        vidcap,
                        frameNumber,
                        frameNumberName,
                        fileNameFolder,
                        block_blob_serviceOUTPUT,
                        containerOutput
                        )
            imagesCreated.append(imageCreated)
    logging.info("Finished looping through all the frames")
    ## Load variables to be returned into one variable
    returnMe = json.dumps([
                            imagesCreated,
                            len(imagesCreated),
                            containerOutput,
                            bsaOutput
                        ])
    return returnMe