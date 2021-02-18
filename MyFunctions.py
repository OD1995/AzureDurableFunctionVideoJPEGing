import os
import re
import pandas as pd
from datetime import datetime, timedelta
import logging
import pyodbc
import cv2
from azure.storage.blob import ContainerPermissions

def getContainer(
    sport,
    container
    ):
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
        return  _sport_
    else:
        return container


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
        return  _sport_,os.getenv("fsecustomvisionimagesConnectionString"),"fsecustomvisionimages"
    else:
        return container,os.getenv("fsevideosConnectionString"),"fsevideos"


def createBlobs(
       vidcap,
       frameNumber,
       frameNumberName,
       event,
       fileName,
       block_blob_service,
       containerOutput,
       multipleVideoEvent
                   ):
    ## Name of video (without ".mp4")
    mp4Name = fileName.split("/")[-1][:-4]
    ## Set the file name to be used
    if event is not None:
        folderName = event
    else:
        ## Blob name without ".mp4"
        folderName = mp4Name

    ## If it's a video from a multi video event, give it a different name
    ##    `multipleVideoEvent` is boolean
    if multipleVideoEvent:
        frameNameEnd = (5 - len(str(frameNumberName)))*"0" + str(frameNumberName)
        frameName = fr"{mp4Name}_{frameNameEnd}.jpeg"
    else:
        frameName = (5 - len(str(frameNumberName)))*"0" + str(frameNumberName) + ".jpeg"
    ## Create path to save image to
    imagePath = fr"{folderName}\{frameName}"
    ## Set the video to the correct frame
    vidcap.set(cv2.CAP_PROP_POS_FRAMES,
                frameNumber)
    logging.info(f"Video set to frame number: {frameNumber}")
    logging.info(f"Frame number name: {frameNumberName}")
    ## Create the image
    success,image = vidcap.read()
    logging.info(f"Image read, success: {success}, `image` type: {type(image)}")
    ## Create variable to be used later on to keep track of which images were generated
    imageCreated = False
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
            ## Image creation successful, so `imageCreated` to True
            imageCreated = True
    
    return imageCreated,frameName

def get_connection_string():
    username = 'matt.shepherd'
    password = "4rsenal!PG01"
    driver = '{ODBC Driver 17 for SQL Server}'
    # driver = 'SQL Server Native Client 11.0'
    server = "fse-inf-live-uk.database.windows.net"
    database = 'AzureCognitive'
    ## Create connection string
    connectionString = f'DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password}'
    
    return connectionString

def getAzureBlobVideos2():
    logging.info("getAzureBlobVideos started")
    ## Create connection string
    connectionString = get_connection_string()
    logging.info(f'Connection string created: {connectionString}')
    ## Create SQL query to use
    sqlQuery = """
                    SELECT      VideoID
                                ,VideoName
                                ,Sport
                                ,Event
                                ,EndpointId
                                ,MultipleVideoEvent
                                ,SamplingProportion
                    FROM        AzureBlobVideos
                """
    with pyodbc.connect(connectionString) as conn:
        ## Get SQL table in pandas DataFrame
        df = pd.read_sql(sql=sqlQuery,
                            con=conn)
    logging.info(f"Dataframe with shape {df.shape} received")
    ## Dict - VideoName : (Sport,Event)
    dfDict = {vn.replace(".mp4","") : (vID,s,e,eID,mve,sp)
                for vID,vn,s,e,eID,mve,sp in zip(
                                    df.VideoID,
                                    df.VideoName,
                                    df.Sport,
                                    df.Event,
                                    df.EndpointId,
                                    df.MultipleVideoEvent,
                                    df.SamplingProportion)}

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
    expiry=datetime.utcnow() + timedelta(days=1)
    )
    return f"{fileURL}?{sasTokenRead}"


def sqlDateTimeFormat(x):
    """
    Convert a datetime object to a SQL-friendly string
    """
    return datetime.strftime(x,'%Y-%m-%d %H:%M:%S')

def sqlColumn_ise(x):
    """
    Prepare string column for SQL
    """
    return f"[{x}]"


def sqlValue_ise(x):
    """
    Prepare int/string value for SQL
    """
    if isinstance(x,str):
        return f"'{x}'"
    elif isinstance(x,int):
        return str(x)
    elif x is None:
        return "NULL"
    else:
        raise ValueError(f'Value is {type(x)}, not str or int')


def execute_sql_command(
    sp_string
):
    connectionString = get_connection_string()
    ## Connect to SQL server
    cursor = pyodbc.connect(connectionString).cursor()
    ## Execute command
    logging.info(sp_string)
    cursor.execute(sp_string)
    ## Get returned values
    rc = cursor.fetchval()
    cursor.commit()

    return rc