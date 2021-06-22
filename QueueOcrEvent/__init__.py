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
from MyFunctions import execute_sql_command


def main(inputs: dict) -> str:

    spSaveOcrRunToCreationQueue_string = f"""
DECLARE	@return_value int

EXEC	@return_value = spSaveOcrRunToCreationQueue
        @JobCreatedBy = '{inputs["JobCreatedBy"]}',
        @JobPriority = {inputs["JobPriority"]},
        @ClientDatabaseId = '{inputs["ClientDatabaseId"]}',
        @EndpointId = '{inputs["EndpointId"]}',
        @Sport = '{inputs["Sport"]}',
        @SportsEvent = '{inputs["SportsEvent"]}',
        @NumberOfImages = {inputs["NumberOfImages"]}

SELECT	'Return Value' = @return_value
    """
    res = execute_sql_command(
        sp_string=spSaveOcrRunToCreationQueue_string,
        database='ThemisDEVELOPMENT'
    )
    logging.info(f"res: `{res}`")

    return "done"