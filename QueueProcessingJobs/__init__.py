# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging
from collections import namedtuple
import json
import os
import sys
sys.path.append(os.path.abspath('.'))
import MyFunctions

QueueDetails = namedtuple('QueueDetails',
                    [
                        'endpointID',
                        'sport',
                        'event',
                        'blobDetails',
                        'frameNumberList',
                        'imagesCreatedList'
                    ])


def main(QD: QueueDetails) -> str:

    (endpointID, sport,
        event, blobDetails,
        frameNumberList0,imagesCreatedList) = QD
    ## Convert strings to lists where needed
    # frameNumberList = json.loads(frameNumberList0)
    # imagesCreatedList = json.loads(imagesCreatedList0)
    ## Get list of frames that were successfully created
    ##    imagesCreatedList - list of True/False, whether that frame was created
    createdFramesList = [
        f
        for f,c in enumerate(imagesCreatedList,1)
        if c
    ]
    ## Get the container to use
    blobOptions = json.loads(blobDetails)
    container = blobOptions['container']
    containerToUse = MyFunctions.getContainer(
    sport=sport,
    container=container
    )


    # AddJobToCloudProcessing_string = """
    # {CALL spComputerVisionCloudProcessing_AddJobToCloudProcessing (?,?,?,?,?,?,?,?,?)}
    # """
    # AddJobToCloudProcessing_string = """
    # EXEC spComputerVisionCloudProcessing_AddJobToCloudProcessing {},{},{},{},{},{},{},{},{}
    # """
    ## Loop through the list of created frames
    for i, frame in enumerate(createdFramesList):
        ## Get Azure image file name
        fileName = (5 - len(str(frame)))*"0" + str(frame) + ".jpeg "
        ## Execute the spComputerVisionCloudProcessing_AddImages stored procedure
        ##     and get the imageID back
        AddImages_string = f"""
        DECLARE	@return_value int

        EXEC	@return_value = [dbo].[spComputerVisionCloudProcessing_AddImages]
                @Sport = '{containerToUse}',
                @Event = '{event}',
                @Filename = '{fileName}'

        SELECT	'Return Value' = @return_value
        """
        # logging.info("AddImages_string")
        # logging.info(AddImages_string)
        imageID = MyFunctions.execute_sql_command(
            sp_string=AddImages_string,
            i=i
        )
        ## Execute the spComputerVisionCloudProcessing_AddJobToCloudProcessing
        ##    stored procedure to add a new row to ComputerVisionProcessingJobs
        # AddJobToCloudProcessing_values = (
        #     "NEWID()", #JobId
        #     "GETUTCDATE()", #JobCreated
        #     "NULL", #JobPickedUp
        #     sqlValue_ise(endpointID), #EndpointId
        #     sqlValue_ise(imageID), #ImageId
        #     sqlValue_ise(sport), #sport
        #     sqlValue_ise(event), #event
        #     sqlValue_ise(fileName), #filename
        #     "NULL", #AzureReadRequestId
        # )
        AddJobToCloudProcessing_string = f"""
        DECLARE	@return_value int

        DECLARE @NewJobId uniqueidentifier
        DECLARE @NewJobCreated datetime2

        SET @NewJobId = NEWID()
        SET @NewJobCreated = GETUTCDATE()

        EXEC	@return_value = [dbo].[spComputerVisionCloudProcessing_AddJobToCloudProcessing]
                @JobId = @NewJobId,
                @JobCreated = @NewJobCreated,
                @JobPickedUp = NULL,
                @EndpointId = '{endpointID}',
                @ImageId = '{imageID}',
                @Sport = '{containerToUse}',
                @Event = '{event}',
                @Filename = '{fileName}',
                @AzureReadRequestId = NULL

        SELECT	'Return Value' = @return_value
        """
        # logging.info("AddJobToCloudProcessing_string")
        # logging.info(AddJobToCloudProcessing_string)
        _ = MyFunctions.execute_sql_command(
            sp_string=AddJobToCloudProcessing_string,
            i=i
        )
        logging.info(f"Image number {i+1} of {len(createdFramesList)} done")

    return f"{len(createdFramesList)} rows added to ComputerVisionProcessingJobs"
