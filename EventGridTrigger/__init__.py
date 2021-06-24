from logging import info
import logging
import azure.functions as func
import azure.durable_functions as df
from HttpTrigger import get_options

async def main(
	event: func.EventGridEvent,
	starter: str
):

	client = df.DurableOrchestrationClient(starter)

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
		instance_id = await client.start_new(
			orchestration_function_name="Orchestrator",
			instance_id=None,
			client_input=options
		)

		logging.info(f"instance_id: {instance_id}")
		logging.info(f"https://futuresvideojpeging.azurewebsites.net/runtime/webhooks/durabletask/instances/{instance_id}?taskHub=TaskHubOD&connection=Storage&code=0HQCgKrzaol22Dawdyxz10UlhIrBQ1sM/yzTRssHrO6hFc03hWRmpA==")
		## code part comes from Function App > Functions > App keys > durabletask_extension
