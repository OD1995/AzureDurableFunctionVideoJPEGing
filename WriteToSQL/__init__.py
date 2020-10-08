# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging
import pyodbc
import os
import sys
sys.path.append(os.path.abspath('.'))
import MyFunctions
from collections import namedtuple
from datetime import datetime

UploadDetails = namedtuple('UploadDetails',
                        ['startUTC',
                        'endUTC',
                        'videoID',
                        'videoName',
                        'event',
                        'outputContainer',
                        'outputBlobStorageAccount',
                        'imagesCreated'])

def main(UD: UploadDetails) -> str:
    (startUTCstr,endUTCstr,videoID,videoName,
    event,outputContainer,
    outputBlobStorageAccount,imagesCreated) = UD
    startUTC = datetime.strptime(startUTCstr,
                                    "%Y-%m-%d %H:%M:%S.%f")
    endUTC = datetime.strptime(endUTCstr,
                                    "%Y-%m-%d %H:%M:%S.%f")
    logging.info("WriteToSQL started")
    ## Get information used to create connection string
    username = 'matt.shepherd'
    password = os.getenv("sqlPassword")
    # password = "4rsenal!PG01"
    driver = '{ODBC Driver 17 for SQL Server}'
    server = os.getenv("sqlServer")
    # server = "fse-inf-live-uk.database.windows.net"
    database = 'AzureCognitive'
    table = 'AzureBlobVideoCompletes'
    ## Create connection string
    connectionString = f'DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password}'
    logging.info(f'Connection string created: {connectionString}')
    ## Create SQL query to use
    cols0 = [
            "StartUTC",
            "EndUTC",
            "VideoID",
            "VideoName",
            "Event",
            "OutputContainer",
            "OutputBlobStorageAccount",
            "ImagesCreated"
            ]
    cols = [MyFunctions.sqlColumn_ise(x) for x in cols0]
    vals0 = [
            MyFunctions.sqlDateTimeFormat(startUTC),
            MyFunctions.sqlDateTimeFormat(endUTC),
            videoID,
            videoName,
            event,
            outputContainer,
            outputBlobStorageAccount,
            imagesCreated
            ]
    vals = [MyFunctions.sqlValue_ise(x) for x in vals0]
    sqlQuery = f"INSERT INTO {table} ({','.join(cols)}) VALUES ({','.join(vals)});"
    with pyodbc.connect(connectionString) as conn:
        with conn.cursor() as cursor:
            logging.info("About to execute 'INSERT' query")
            cursor.execute(sqlQuery)
            logging.info("'INSERT' query executed")

    return f"Row added to {table}"