import logging
import azure.functions as func
import azure.durable_functions as df
import os
import sys
sys.path.append(os.path.abspath('.'))
from MyFunctions import execute_sql_command
from urllib.parse import unquote

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
					'blob',
					'imagesAlreadyCreated'
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
			"blob": subject[6],
			'imagesAlreadyCreated' : None
					}

	if user == 'FromQueue':
		options = {}
		for f in [  
					'fileUrl',
					'imagesAlreadyCreated',
					'RowID'
					]:
			options[f] = req.params.get(f)
		
		options['container'] = options['fileUrl'].split("/")[-2]
		options['blob'] = unquote(options['fileUrl'].split("/")[-1])

	logging.info("starter----------------> %s",options)

	return options

async def main(
	req: func.HttpRequest,
    starter: str
):
    
	# client = df.DurableOrchestrationClient(starter)

	options = get_options(
		user='HttpTrigger',
		req=req
	)
	if options['container'] not in [
        "us-office",
        "azure-video-to-image-import"
	]:
		return f"Container = `{options['container']}` so no processing needed"
	else:
		# instance_id = await client.start_new(
		# 	orchestration_function_name="Orchestrator",
		# 	instance_id=None,
		# 	client_input=options
		# )

		# return client.create_check_status_response(
		# 	req,
		# 	instance_id
		# )
		fileURL = options['fileUrl'].replace("'","''")
		imagesAlreadyCreated = options['imagesAlreadyCreated']
		if imagesAlreadyCreated is None:
			imagesAlreadyCreated = "NULL"
		## Create row in VideoJPEGingQueue
		iQ = f"""
		INSERT INTO VideoJPEGingQueue ([FileURL],[ImagesAlreadyCreated],[QueuedMethod])
		VALUES ('{fileURL}',{imagesAlreadyCreated},'HttpTrigger')
		"""
		execute_sql_command(
			sp_string=iQ,
			database="PhotoTextTrack",
			return_something=False
		)