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
import MyFunctions


def main(inputs: dict) -> str:

    spSaveOcrRunToCreationQueue_string = f"""
EXEC    spSaveOcrRunToCreationQueue
        @JobCreatedBy = '{inputs["JobCreatedBy"]}',
        @JobPriority = {inputs["JobPriority"]},
        @ClientDatabaseId = '{inputs["ClientDatabaseId"]}',
        @EndpointId = '{inputs["EndpointId"]}',
        @Sport = '{inputs["Sport"]}',
        @SportsEvent = '{inputs["SportsEvent"]}',
        @NumberOfImages = {inputs["NumberOfImages"]}
    """
    _ = MyFunctions.execute_sql_command(
        sp_string=spSaveOcrRunToCreationQueue_string
    )

    return "done"