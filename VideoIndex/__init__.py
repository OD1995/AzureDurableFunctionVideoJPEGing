# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging
import os
import sys
sys.path.append(os.path.abspath('.'))
from MyFunctions import (
    get_SAS_URL,
    get_url_container_and_file_name
)
from azure.storage.blob import BlockBlobService


def main(inputs: dict) -> str:
    """    
    Copy the video into the videoindexer-files container, which will trigger the 
    system of 3 functions to eventually download the transcript to SQL
    """ 
    ## Set inputs
    vidURL = inputs['fileURL']
    urlContainer,urlFileName = get_url_container_and_file_name(vidURL)
    bbs = BlockBlobService(
        connection_string=os.getenv("fsevideosConnectionString")
    )
    ## Create SAS URL
    sasURL = get_SAS_URL(
                        fileURL=vidURL,
                        block_blob_service=bbs,
                        container=urlContainer
                    )
    ## Copy blob
    bbs.copy_blob(
        container_name="videoindexer-files",
        blob_name=urlFileName,
        copy_source=sasURL
    )

    return "done"
