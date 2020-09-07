from sqlalchemy import create_engine
import pandas as pd
import os
import re

def getAzureBlobVideos():
    username = 'matt.shepherd'
    password = os.getenv("sqlPassword")
    driver= 'SQL Server Native Client 11.0'
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

def checkOrFixContainerName(sport):

    ## # Make some adjustments to make the container name as ready as possible
    ## Convert all `sport` characters to lower case
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


    if length & rightCharTypes & firstCharRight & lastCharRight:
        returnMe =  _sport_
    else:
        returnMe = False

    return returnMe