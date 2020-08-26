# This function is not intended to be invoked directly. Instead it will be
# triggered by an HTTP starter function.
# Before running this sample, please:
# - create a Durable activity function (default name is "Hello")
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging
import json
from collections import namedtuple

import azure.functions as func
import azure.durable_functions as df


def orchestrator_function(context: df.DurableOrchestrationContext):
    ## Get time to cut from, using MLB API
    timeToCut = yield context.call_activity(name='CallAPI',
                                            input_=context._input)
    
    ## Convert video to JPEGs, ending at `timeToCut`
    ##    Use composite object
    ##     - https://docs.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-orchestrations?tabs=python#passing-multiple-parameters
    vidDets = namedtuple('VideoDetails',
                         ['fileName',
                          'timeToCut'])
    videoDetails = vidDets(blobDetails=context._input,
                           timeToCut=timeToCut)
    jpegFolder = yield context.call_activity(name='MP4toJPEGs',
                                                input_=videoDetails)

    return jpegFolder

main = df.Orchestrator.create(orchestrator_function)