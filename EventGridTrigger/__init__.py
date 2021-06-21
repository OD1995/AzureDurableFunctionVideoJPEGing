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

    instance_id = await client.start_new(orchestration_function_name="Orchestrator",
                                            instance_id=None,
                                            client_input=options)
