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
        frameNumberList0,imagesCreatedList0) = QD
    ## Convert strings to lists where needed
    frameNumberList = json.loads(frameNumberList0)
    imagesCreatedList = json.loads(imagesCreatedList0)
    ## Get list of frames that were successfully created
    createdFramesList = [
        f
        for f,c in zip(frameNumberList,imagesCreatedList)
        if c
    ]
    ## Define string to use to execut stored procedure
    AddImages_string = """
    DECLARE	@return_value int

    EXEC	@return_value = [dbo].[spComputerVisionCloudProcessing_AddImages]
            @Sport = ?,
            @Event = ?,
            @Filename = ?

    SELECT	'Return Value' = @return_value
    """
    AddJobToCloudProcessing_string = """
    EXEC	@JobId = ?
            @JobCreated = ?,
            @JobPickedUp = ?,
            @EndpointId = ?,
            @ImageId = ?,
            @Sport = ?,
            @Event = ?,
            @Filename = ?,
            @AzureReadRequestId = ?
    """
    ## Loop through the list of created frames
    for frame in createdFramesList:
        ## Get Azure image file name
        fileName = (5 - len(str(frame)))*"0" + str(frame)
        ## Execute the spComputerVisionCloudProcessing_AddImages stored procedure
        ##     and get the imageID back
        AddImages_values = (
            sport,
            event,
            fileName
        )
        imageID = MyFunctions.execute_sql_command(
            sp_string=AddImages_string,
            sp_values=AddImages_values
        )
        ## Execute the spComputerVisionCloudProcessing_AddJobToCloudProcessing
        ##    stored procedure to add a new row to ComputerVisionProcessingJobs
        AddJobToCloudProcessing_values = (
            "NEWID()", #JobId
            "GETUTCDATE()", #JobCreated
            "NULL", #JobPickedUp
            endpointID, #EndpointId
            imageID, #ImageId
            sport, #sport
            event, #event
            fileName, #filename
            "NULL", #AzureReadRequestId
        )
        _ = MyFunctions.execute_sql_command(
            sp_string=AddJobToCloudProcessing_string,
            sp_values=AddJobToCloudProcessing_values
        )

    return f"{len(createdFramesList)} rows added to ComputerVisionProcessingJobs"
