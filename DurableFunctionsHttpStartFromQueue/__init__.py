# This function an HTTP starter function for Durable Functions.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable activity function (default name is "Hello")
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt
 
import logging
from HttpTrigger import get_options
import azure.functions as func
import azure.durable_functions as df
import json
import os
import sys
sys.path.append(os.path.abspath('.'))
from MyFunctions import update_row_status


async def main(req: func.HttpRequest, starter: str) -> func.HttpResponse:
    ## `req` requires parameters:
					# 'fileUrl',
					# 'imagesAlreadyCreated',
					# 'RowID'
    client = df.DurableOrchestrationClient(starter)
    options = get_options(
        user='FromQueue',
        req=req
    )
    logging.info(f"options: {options}")
    instance_id = await client.start_new(
    	orchestration_function_name="Orchestrator",
    	instance_id=None,
    	client_input=options
    )
    csr = client.create_check_status_response(
        req,
        instance_id
    )
    statusQueryGetUri = json.loads(csr.get_body()).get('statusQueryGetUri')

    update_row_status(
        rowID=options['RowID'],
        uri=statusQueryGetUri
    )
    update_row_status(
        rowID=options['RowID'],
        status='Starting'
    )

    return client.create_check_status_response(req, instance_id)