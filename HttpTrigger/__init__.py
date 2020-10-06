import logging

import azure.functions as func
import azure.durable_functions as df


def main(req: func.HttpRequest,
            starter: str):

    client = df.DurableOrchestrationClient(starter)

    options = {}
    for f in [  
                'fileUrl',
                'container',
                'blob'
                ]:
        options[f] = req.params.get(f)



    logging.info("starter----------------> %s",options)

    instance_id = await client.start_new(orchestration_function_name="Orchestrator",
                                            instance_id=None,
                                            client_input=options)
