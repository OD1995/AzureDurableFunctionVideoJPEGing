import logging
import azure.functions as func
import azure.durable_functions as df
import json

def get_options(
	user,
	req=None,
	event=None
):
	if user == 'HttpTrigger':
		options = {}
		for f in [  
					'fileUrl',
					'container',
					'blob'
					]:
			options[f] = req.params.get(f)


	elif user == 'EventGridTrigger':
		## File path (from event.subject) takes below form
		##    "/blobServices/default/containers/{CONTAINER_NAME}/blobs/{BLOB_NAME}"
		## Blob name (file name) will include folder if necessary

		subject = (event.subject).split('/')
		options = {
			"fileUrl": event.get_json()['url'],
			"container": subject[4],
			"blob": subject[6]
					}

	logging.info("starter----------------> %s",options)

	return options

async def main(
	req: func.HttpRequest,
    starter: str
):
    
	client = df.DurableOrchestrationClient(starter)

	options = get_options(
		user='HttpTrigger',
		req=req
	)

    instance_id = await client.start_new(orchestration_function_name="Orchestrator",
                                            instance_id=None,
                                            client_input=options)

    return client.create_check_status_response(req, instance_id)