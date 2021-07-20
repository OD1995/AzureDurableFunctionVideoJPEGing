from logging import info
import logging
import azure.functions as func
import azure.durable_functions as df
from HttpTrigger import get_options
import os
import sys
sys.path.append(os.path.abspath('.'))
from MyFunctions import execute_sql_command

async def main(
	event: func.EventGridEvent,
	starter: str
):

	# client = df.DurableOrchestrationClient(starter)

	options = get_options(
		user='EventGridTrigger',
		event=event
	)
	if options['container'] not in [
        "us-office",
        "azure-video-to-image-import"
    ]:
		logging.info(f"Container = `{options['container']}` so no processing needed")
	else:
		# instance_id = await client.start_new(
		# 	orchestration_function_name="Orchestrator",
		# 	instance_id=None,
		# 	client_input=options
		# )

		# logging.info(f"instance_id: {instance_id}")
		# logging.info(f"https://futuresvideojpeging.azurewebsites.net/runtime/webhooks/durabletask/instances/{instance_id}?taskHub=TaskHubOD&connection=Storage&code=0HQCgKrzaol22Dawdyxz10UlhIrBQ1sM/yzTRssHrO6hFc03hWRmpA==")
		## code part comes from Function App > Functions > App keys > durabletask_extension
		fileURL = options['fileUrl'].replace("'","''")
		imagesAlreadyCreated = options['imagesAlreadyCreated']
		if imagesAlreadyCreated is None:
			imagesAlreadyCreated = "NULL"
		## Create row in VideoJPEGingQueue
		iQ = f"""
		INSERT INTO VideoJPEGingQueue ([FileURL],[ImagesAlreadyCreated],[QueuedMethod])
		VALUES ('{fileURL}',{imagesAlreadyCreated},'EventGridTrigger')
		"""
		execute_sql_command(
			sp_string=iQ,
			database="PhotoTextTrack",
			return_something=False
		)