import os
import re
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime

def getAzureBlobVideos():
    username = 'matt.shepherd'
    password = os.getenv("sqlPassword")
    driver= 'SQL+Server'
    server = os.getenv("sqlServer")
    database = 'AzureCognitive'
    table = 'AzureBlobVideos'

    connectionString = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}'
    ## Create engine
    engine = create_engine(connectionString,
                        fast_executemany=True)
    ## Make connection
    conn = engine.connect()
    ## Get SQL table in pandas DataFrame
    df = pd.read_sql_table(table_name=table,
                            con=conn)
    ## Close connection
    conn.close()  
    ## Dict - VideoName : (Sport,Event) 
    dfDict = {vn : (s,e)
                for vn,s,e in zip(df.VideoName,
                                    df.Sport,
                                    df.Event)}

    return dfDict

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


def cleanUpVidName(videoName0):
    """Clean up video name if '_HHMM-YYYY-mm-dd.mp4' is in the video name"""
    try:
        _ = datetime.strptime(videoName0[-15:-4],
                                "-%Y-%m-%d")
        return videoName0.replace(videoName0[-15:-4],"")
    except ValueError:
        return videoName0