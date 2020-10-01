import os
import re
#from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime, timedelta
import logging
#import socket
import pyodbc
import cv2
from azure.storage.blob import ContainerPermissions


#def getAzureBlobVideos():
#    logging.info("getAzureBlobVideos started")
#    ## Get information used to create connection string
#    username = 'matt.shepherd'
#    password = os.getenv("sqlPassword")
#    driver = 'SQL+Server'
#    server = os.getenv("sqlServer")
#    database = 'AzureCognitive'
#    table = 'AzureBlobVideos'
#    ## Create connection string
#    connectionString = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}'
#    logging.info(f'Connection string created: {connectionString}')
#    logging.info(f"IP address: {socket.gethostbyname(socket.getfqdn())}")
#    ## Create engine
#    engine = create_engine(connectionString,
#                        fast_executemany=True)
#    logging.info('engine created')
#    ## Make connection
#    conn = engine.connect()
#    logging.info('engine connected')
#    ## Get SQL table in pandas DataFrame
#    df = pd.read_sql_table(table_name=table,
#                            con=conn)
#    ## Close connection
#    conn.close()  
#    ## Dict - VideoName : (Sport,Event) 
#    dfDict = {vn : (s,e)
#                for vn,s,e in zip(df.VideoName,
#                                    df.Sport,
#                                    df.Event)}
#
#    return dfDict


def getContainerAndConnString(sport,
                                container):
    """
    Using the sport value from the AzureBlobVideos SQL table and
    the container the MP4 is currently in, work out which container
    and blob storage account to insert images into
    """
    ## # Make some adjustments to make the container name as ready as possible
    ## Convert all `sport` characters to lower case
    if sport is not None:
        isNotNone = True
        _sport_ = "".join([x.lower() if isinstance(x,str)
                            else "" if x == " " else x
                            for x in sport])
        ## Replace double hyphens
        _sport_ = _sport_.replace("--","-").replace("--","-")

        ## # Make some checks
        ## Check that the length is between 3 and 63 charachters
        length = (len(_sport_) >= 3) & (len(_sport_) <= 63)
        ## Check that all characters are either a-z, 0-9 or -
        rightCharTypes = True if re.match("^[a-z0-9-]*$", _sport_) else False
        ## Check that the first character is either a-z or 0-9
        firstCharRight = True if re.match("^[a-z0-9]*$", _sport_[0]) else False
        ## Check that the last character is either a-z or 0-9
        lastCharRight = True if re.match("^[a-z0-9]*$", _sport_[-1]) else False
    else:
        isNotNone = False
        length = False
        rightCharTypes = False
        firstCharRight = False
        lastCharRight = False
        _sport_ = ""



    if isNotNone & length & rightCharTypes & firstCharRight & lastCharRight:
        return  _sport_,os.getenv("fsecustomvisionimagesConnectionString")
    else:
        return container,os.getenv("fsevideosConnectionString")


def createBlobs(
       vidcap,
       frameNumber,
       frameNumberName,
       fileNameFolder,
       block_blob_service,
       containerOutput
                   ):
    ## Create path to save image to
    frameName = (5 - len(str(frameNumberName)))*"0" + str(frameNumberName)
    imagePath = fr"{fileNameFolder}\{frameName}.jpeg"
    ## Set the video to the correct frame
    vidcap.set(cv2.CAP_PROP_POS_FRAMES,
                frameNumber)
    logging.info(f"Video set to frame number: {frameNumber}")
    ## Create the image
    success,image = vidcap.read()
    logging.info(f"Image read, success: {success}, `image` type: {type(image)}")
    if success:
        ## Encode image
        success2, image2 = cv2.imencode(".jpeg", image)
        logging.info(f"Image encoded, success2: {success2}, `image2` type: {type(image2)}")
        if success2:
            ## Convert image2 (numpy.ndarray) to bytes
            byte_im = image2.tobytes()
            logging.info("Image converted to bytes")
            ## Create the new blob
            block_blob_service.create_blob_from_bytes(container_name=containerOutput,
                                                        blob_name=imagePath,
                                                        blob=byte_im)
            logging.info(f"Blob ({imagePath}) created....")

def getAzureBlobVideos2():
    logging.info("getAzureBlobVideos started")
    ## Get information used to create connection string
    username = 'matt.shepherd'
    # password = os.getenv("sqlPassword")
    password = "4rsenal!PG01"
    driver = '{ODBC Driver 17 for SQL Server}'
    # server = os.getenv("sqlServer")
    server = "fse-inf-live-uk.database.windows.net"
    database = 'AzureCognitive'
    table = 'AzureBlobVideos'
    ## Create connection string
    connectionString = f'DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password}'
    logging.info(f'Connection string created: {connectionString}')
    ## Create SQL query to use
    sqlQuery = f"SELECT * FROM {table}"
    with pyodbc.connect(connectionString) as conn:
        ## Get SQL table in pandas DataFrame
        df = pd.read_sql(sql=sqlQuery,
                            con=conn)
    logging.info(f"Dataframe with shape {df.shape} received")
    ## Dict - VideoName : (Sport,Event)
    try:
        dfDict = {vn : (s,e)
                    for vn,s,e in zip(df.VideoName,
                                        df.Sport,
                                        df.Event)}
    except:
        logging.info("Error received")

    return dfDict

def cleanUpVidName(videoName0):
    """Clean up video name if '_HHMM-YYYY-mm-dd.mp4' is in the video name"""
    try:
        _ = datetime.strptime(videoName0[-15:-4],
                                "-%Y-%m-%d")
        return videoName0.replace(videoName0[-15:-4],"")
    except ValueError:
        return videoName0

def get_SAS_URL(fileURL,
                block_blob_service,
                container):

    sasTokenRead = block_blob_service.generate_container_shared_access_signature(
    container_name=container,
    permission=ContainerPermissions.READ,
    expiry=datetime.utcnow() + timedelta(hours=1)
    )
    return f"{fileURL}?{sasTokenRead}"