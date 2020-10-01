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
import re
import sys
sys.path.append(os.path.abspath('.'))
import MyClasses
import tempfile

vidDets = namedtuple('VideoDetails',
                        ['blobDetails',
                         'timeToCut'
                         'frameNumberList',
                         'sport',
                         'event'])

def createBlobs(
       vidcap,
       frameNumber,
       frameNumberName,
       fileNameFolder,
       block_blob_service,
       containerOutput
                   ):
    ## Create path to save image to
    frameName = (5 - len(str(frameNumberName)))*"0" + str(frameNumberName)
    imagePath = fr"{fileNameFolder}\{frameName}.jpeg"
    ## Set the video to the correct frame
    vidcap.set(cv2.CAP_PROP_POS_FRAMES,
                frameNumber)
    logging.info(f"Video set to frame number: {frameNumber}")
    ## Create the image
    success,image = vidcap.read()
    logging.info(f"Image read, success: {success}, `image` type: {type(image)}")
    if success:
        ## Encode image
        success2, image2 = cv2.imencode(".jpeg", image)
        logging.info(f"Image encoded, success2: {success2}, `image2` type: {type(image2)}")
        if success2:
            ## Convert image2 (numpy.ndarray) to bytes
            byte_im = image2.tobytes()
            logging.info("Image converted to bytes")
            ## Create the new blob
            block_blob_service.create_blob_from_bytes(container_name=containerOutput,
                                                        blob_name=imagePath,
                                                        blob=byte_im)
            logging.info(f"Blob ({imagePath}) created....")

def getContainerAndConnString(sport,
                                container):
    """
    Using the sport value from the AzureBlobVideos SQL table and
    the container the MP4 is currently in, work out which container
    and blob storage account to insert images into
    """
    ## # Make some adjustments to make the container name as ready as possible
    ## Convert all `sport` characters to lower case
    if sport is not None:
        isNotNone = True
        _sport_ = "".join([x.lower() if isinstance(x,str)
                            else "" if x == " " else x
                            for x in sport])
        ## Replace double hyphens
        _sport_ = _sport_.replace("--","-").replace("--","-")

        ## # Make some checks
        ## Check that the length is between 3 and 63 charachters
        length = (len(_sport_) >= 3) & (len(_sport_) <= 63)
        ## Check that all characters are either a-z, 0-9 or -
        rightCharTypes = True if re.match("^[a-z0-9-]*$", _sport_) else False
        ## Check that the first character is either a-z or 0-9
        firstCharRight = True if re.match("^[a-z0-9]*$", _sport_[0]) else False
        ## Check that the last character is either a-z or 0-9
        lastCharRight = True if re.match("^[a-z0-9]*$", _sport_[-1]) else False
    else:
        isNotNone = False
        length = False
        rightCharTypes = False
        firstCharRight = False
        lastCharRight = False
        _sport_ = ""



    if isNotNone & length & rightCharTypes & firstCharRight & lastCharRight:
        return  _sport_,os.getenv("fsecustomvisionimagesConnectionString")
    else:
        return container,os.getenv("fsevideosConnectionString")


def main(videoDetails: vidDets) -> str:
    ## Get blob details
    blobDetails,timeToCut,frameNumberList0,sport,event = videoDetails
    blobOptions = json.loads(blobDetails)
    container = blobOptions['container']
    fileURL = blobOptions['fileUrl']
    fileName = blobOptions['blob']
    frameNumberList = json.loads(frameNumberList0)
    logging.info(f"frameNumberList (type: {type(frameNumberList)}, length: {len(frameNumberList)}) received")
    # ## Get clean video name to be used as folder name (without ".mp4" on the end)
    # vidName = MyFunctions.cleanUpVidName(fileName.split("/")[-1])[:-4]
    ## Return the container name and connection string to insert images into
    containerOutput, connectionStringOutput = getContainerAndConnString(
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
    ## Create BlockBlobService object to be used to upload blob to container
    block_blob_service = BlockBlobService(connection_string=connectionStringOutput)
    logging.info(f'BlockBlobService created for account "{block_blob_service.account_name}"')
    ## Get names of all containers in the blob storage account
    containerNames = [x.name
                        for x in block_blob_service.list_containers()]
    logging.info(f"List of containerNames received, length: {len(containerNames)}")
    ## Create container (will do nothing if container already exists)
    if containerOutput not in containerNames:
        logging.info(f"Container '{containerOutput}' doesn't exist")
        existsAlready = block_blob_service.create_container(container_name=containerOutput,
                                                            fail_on_exist=False)
        logging.info(f"Container '{containerOutput}' didn't exist, now has been created")
    else:
        logging.info(f"Container '{containerOutput}' exists already'")
    ## Open the video
    vidcap = cv2.VideoCapture(fileURL)
    logging.info("VideoCapture object created")
    success,image = vidcap.read()
    ## Get metadata
    fps = vidcap.get(cv2.CAP_PROP_FPS)
    frameCount = int(vidcap.get(cv2.CAP_PROP_FRAME_COUNT))
    logging.info('Video metadata acquired')
    logging.info(f"frameCount: {str(frameCount)}")
    logging.info(f"FPS: {fps}")
    ## If frame count negative, download locally and try again
    if frameCount <= 0:
        logging.info("Frame count greater than 0, so local download needed (MP4toJPEGs)")
        with tempfile.TemporaryDirectory() as dirpath:
            ## Get blob and save to local directory
            vidLocalPath = fr"{dirpath}\{fileName}"
            # logging.info("About to get connection string")
            # logging.info(f"CS: {os.environ['fsevideosConnectionString']}")
            fsevideosConnectionString = "DefaultEndpointsProtocol=https;AccountName=fsevideos;AccountKey=xfYncTDRCowSrISbdsSknM05jqOrJXc4Oavq7BQ56yR7uQ7MCeL5aXmBsbsE+SZ+++xGt2oy6FvrEdpryc+vwQ==;EndpointSuffix=core.windows.net"
            logging.info("About to create BlockBlobService")
            block_blob_service = BlockBlobService(connection_string=fsevideosConnectionString)
            logging.info("BlockBlobService created")
            block_blob_service.get_blob_to_path(container_name=container,
                                                blob_name=fileName,
                                                file_path=vidLocalPath)
            logging.info("Blob saved to path")
            with MyClasses.MyVideoCapture(vidLocalPath) as vc1:
                for frameNumberName,frameNumber in enumerate(frameNumberList,1):
                    ## Create blobs
                    createBlobs(
                                vc1,
                                frameNumber,
                                frameNumberName,
                                fileNameFolder,
                                block_blob_service,
                                containerOutput
                                )
    else:
        ## Loop through the frame numbers
        for frameNumberName,frameNumber in enumerate(frameNumberList,1):
            ## Create blobs
            createBlobs(
                        vidcap,
                        frameNumber,
                        frameNumberName,
                        fileNameFolder,
                        block_blob_service,
                        containerOutput
                        )
    logging.info("Finished looping through frames")
    return True