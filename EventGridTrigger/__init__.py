import json
import logging

import azure.functions as func
import azure.durable_functions as df


def main(event: func.EventGridEvent,
            starter: str):

    client = df.DurableOrchestrationClient(starter)

    ## File path (from event.subject) takes below form
    ##    "/blobServices/default/containers/{CONTAINER_NAME}/blobs/{BLOB_NAME}"
    ## Blob name (file name) will include folder if necessary

    subject = (event.subject).split('/')
    options = {
        "fileUrl": event.data['fileUrl'],
        "container": subject[4],
        "blob": subject[6]
                }

    logging.info("starter----------------> %s",options)

    instance_id = await client.start_new(orchestration_function_name="Orchestrator",
                                            instance_id=None,
                                            client_input=options)
