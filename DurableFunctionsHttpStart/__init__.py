# This function an HTTP starter function for Durable Functions.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable activity function (default name is "Hello")
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt
 
import logging

import azure.functions as func
import azure.durable_functions as df


async def main(req: func.HttpRequest,
                context: func.Context,
                starter: str) -> func.HttpResponse:
    client = df.DurableOrchestrationClient(starter)
    instance_id = await client.start_new(orchestration_function_name=req.route_params["functionName"],
                                            instance_id=None,
                                            client_input=None)
    loggingString = f"Started orchestration with ID = '{instance_id}'."
    loggingString += f"\n Function directory: '{context.function_name}'"
    loggingString += f"\n Function name: '{context.function_name}'"
    loggingString += f"\n Function invocation ID '{context.invocation_id}'"
    logging.info(loggingString)

    return client.create_check_status_response(req, instance_id)