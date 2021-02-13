import requests
import pandas as pd
import numpy as np
from pandas import json_normalize as jn
import mysql
import mysql.connector
from mysql.connector import errorcode
from sqlalchemy import create_engine
from datetime import datetime
from datetime import timedelta
from pandas.tseries.offsets import DateOffset

# create new database
def createDB(engine, db_name):
    try:
        engine.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(db_name))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)

# create tables in mysql
def createTable(engine, tables):
    for table_name in tables:
        table_des = tables[table_name]
        try:
            print("creating table {}: ".format(table_name), end='')
            engine.execute(table_des)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)
        else:
            print("OK")

def incrementalLoadDf(startdate, enddate, engine):
    usgs_request_url = "https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson"
    usgs_request_url += "&starttime=" + startdate + "&endtime=" + enddate
    print(usgs_request_url)
    geojsonrecord = requests.get(usgs_request_url).json()
    json_norm = jn(geojsonrecord)

    conn = engine.raw_connection()
    cursor = conn.cursor()

    retries = 1
    success = False
    while not success:
        try:
            response = urllib2.urlopen(request)
            success = True
        except Exception as e:
            wait = retries * 30;
            print
            'Error! Waiting %s secs and re-trying...' % wait
            sys.stdout.flush()
            time.sleep(wait)
            retries += 1


    # get max pid number as the new pid start number feed
    sql_get_max_id = "select ifnull(max(pid),0) from properties;"
    cursor.execute(sql_get_max_id)
    max_id = cursor.fetchall()[0]

    # properties_df
    features_norm = jn(json_norm["features"][0])

    properties_df = features_norm.drop(columns=["type", "properties.url", "properties.detail", "geometry.type", "geometry.coordinates"])
    col_name = {'properties.mag':'mag', 'properties.place':'place',   'properties.time':'time',
                'properties.updated':'updated',   'properties.tz':'tz',      'properties.felt':'felt',
                'properties.cdi':'cdi',       'properties.mmi':'mmi',     'properties.alert':'alert',
                'properties.status':'status',    'properties.tsunami':'tsunami', 'properties.sig':'sig',
                'properties.net':'net',       'properties.code':'code',    'properties.ids':'ids',
                'properties.sources':'sources',   'properties.types':'types',   'properties.nst':'nst',
                'properties.dmin':'dmin',      'properties.rms':'rms',     'properties.gap':'gap',
                'properties.magType':'magType',   'properties.type':'type',    'properties.title':'title'}
    properties_df = properties_df.rename(columns=col_name)
    properties_df["time"] = pd.to_datetime(properties_df['time'], unit='ms')
    properties_df["updated"] = pd.to_datetime(properties_df['updated'], unit='ms')
    properties_df["pid"] = properties_df.index + max_id + 1


    # split categorical columns to build relational database
    # the database structure need to follow 3NF
    # new tables would be
    # types, types_properties, sources, sources_properties, related_events
    sources_hash = {"sources":[], "id_str":[]}
    types_hash = {"types":[], "pid":[]}
    events_hash = {"original_event_pid":[], "related_event_id_str":[]}

    for index, row in properties_df.iterrows():
        # data cleaning
        # remove start/end comma
        sources = row["sources"].lstrip(',').rstrip(',').split(",")
        related_id = row["ids"].lstrip(',').rstrip(',').split(",")
        types = row["types"].lstrip(',').rstrip(',').split(",")
        id = row["id"]
        pid = row["pid"]
        # the source from network contributors, and each source related to one related id
        # loop properties_df split [sources] and [ids] insert into sources df

        # generate sources_hash prepared to insert into sources_df
        for _ in range(len(sources)):
            sources_hash["sources"].append(sources[_])
            sources_hash["id_str"].append(related_id[_])

        # generate types_hash prepared to insert into types_df
        for _ in range(len(types)):
            types_hash["types"].append(types[_])
            types_hash["pid"].append(pid)

        for _ in range(len(related_id)):
            events_hash["original_event_pid"].append(pid)
            events_hash["related_event_id_str"].append(related_id[_])

    # sources_properties_df
    sources_properties_df = pd.DataFrame(data=sources_hash)
    sources_properties_df = sources_properties_df.drop_duplicates()
    # sources_df
    sources_df = sources_properties_df["sources"].drop_duplicates()
    # types_properties_df
    types_properties_df = pd.DataFrame(data=types_hash)
    # types_df
    types_df = types_properties_df["types"].drop_duplicates()
    # related_event_df
    related_event_df = pd.DataFrame(data=events_hash)
    # geometry df
    geometry_df = pd.DataFrame(features_norm["geometry.coordinates"].tolist(), columns=["longitude", "latitude", "depth"] )
    geometry_df['id'] = properties_df['id']
    geometry_df['pid'] = properties_df['pid']

    cursor.close()
    return properties_df, geometry_df, sources_df, sources_properties_df, types_df, types_properties_df, related_event_df

def fullLoadMySQL(df, table_name, engine):
    df.to_sql(table_name, con=engine, if_exists='append', index=False)

"""
************************************************************************************
Main script part below
************************************************************************************
"""
# table structure
tables = {}
tables["properties"] = (
    "CREATE table properties ("
    "   id        VARCHAR(255),"
    "   mag       FLOAT,"
    "   place     VARCHAR(255),"
    "   time      VARCHAR(255),"
    "   updated   VARCHAR(255),"
    "   tz        FLOAT,"
    "   felt      FLOAT,"
    "   cdi       FLOAT,"
    "   mmi       FLOAT,"
    "   alert     VARCHAR(255),"
    "   status    VARCHAR(255),"
    "   tsunami   BIGINT,"
    "   sig       BIGINT,"
    "   net       VARCHAR(255),"
    "   code      VARCHAR(255),"
    "   ids       VARCHAR(255),"
    "   sources   VARCHAR(255),"
    "   types     VARCHAR(255),"
    "   nst       FLOAT,"
    "   dmin      FLOAT,"
    "   rms       FLOAT,"
    "   gap       FLOAT,"
    "   magType   VARCHAR(255),"
    "   type      VARCHAR(255),"
    "   title     VARCHAR(255),"
    "   pid       INT,"
    "PRIMARY KEY (pid)"
    ");"
)

tables["sources"] = (
    "CREATE table sources ("
    "   sid     INT NOT NULL AUTO_INCREMENT,"
    "   sources  varchar(20),"
    "PRIMARY KEY (sid)"
    ");"
)

tables["sources_properties"] = (
    "CREATE table sources_properties ("
    "   sid     INT,"
    "   pid     INT"
    ");"
)

tables["types"] = (
    "CREATE table types ("
    "   tid     INT NOT NULL AUTO_INCREMENT,"
    "   types    varchar(255),"
    "PRIMARY KEY (tid)"
    ");"
)

tables["types_properties"] = (
    "CREATE table types_properties ("
    "   tid     INT,"
    "   pid     INT"
    ");"
)

tables["related_events"] = (
    "CREATE table related_events ("
    "   original_event_pid      INT,"
    "   related_event_id_str    VARCHAR(255)"
    ");"
)

tables["geometry"] = (
    "CREATE TABLE geometry ("
    "   longitude FLOAT,"
    "   latitude  FLOAT,"
    "   depth     FLOAT,"
    "   id        VARCHAR(255),"
    "   pid       INT  NOT NULL,"  
    "PRIMARY KEY (pid)"
    ");"
)

# set mysql connection string
username = "mwu"
password = "mwu"
host = "localhost"
port = "3306"
connectionString = "mysql://" + username + ":" + password + "@" + host + ":" + port + "/"
engine = create_engine(connectionString)

# create database
# if db exists using db, if not create db
db_name = "usgs1"
createDB(engine, db_name)
try:
    engine.execute("USE {}".format(db_name))
    # cursor.execute("USE {}".format(db_name))
except mysql.connector.Error as err:
    print("Database {} does not exists.".format(db_name))
    if err.errno == errorcode.ER_BAD_DB_ERROR:
        createDB(engine, db_name)
        print("Database {} created successfully.".format(db_name))
        # cnx.database = db_name
    else:
        print(err)
        exit(1)


# refresh connection string and engine
connectionString += db_name
engine = create_engine(connectionString)

# create tables
createTable(engine, tables)

# initial start_datetime/end_datetime as timestamp
start_datetime = pd.Timestamp('2017-01-01').date()
end_datetime = pd.Timestamp('2017-01-02').date()
# download data, load into DF, load into MySQL
while start_datetime < end_datetime:
    # convert start_datetime, end_datetime to str
    start_date = str(start_datetime)
    end_date = str(pd.Timestamp(start_datetime + DateOffset(months=1)).date())

    # incremental load data from usgs api to dataframe
    properties_df, geometry_df, sources_df, sources_properties_df, types_df, types_properties_df, related_event_df = incrementalLoadDf(start_date, end_date, engine)

    # incremental load from dataframe into MySQL
    fullLoadMySQL(properties_df, "properties", engine)

    # incremental load sources
    # check distinct value in sources table
    # output into df
    conn = engine.raw_connection()
    cursor = conn.cursor()
    sql = "select distinct sources from usgs.sources;"
    cursor.execute(sql)
    existing_source_df = pd.DataFrame(data=cursor.fetchall(), columns=['exist_sources'])
    cursor.close()

    # use sources_df left join sources_df
    if existing_source_df.empty:
        sources_df.to_sql("sources", con=engine, if_exists='append', index=False)
    else:
        sources_df2 = pd.DataFrame(data=sources_df, columns=["sources"])
        sources_df3 = pd.merge(sources_df2, existing_source_df, left_on='sources', right_on='exist_sources')
        sources_df4 = sources_df3["sources"][sources_df3["sources"].isna()]
        if not sources_df4.empty:
            sources_df4.to_sql("sources", con=engine, if_exists='append', index=False)

    # incremental load types
    # conn = engine.raw_connection()
    conn = engine.raw_connection()
    cursor = conn.cursor()
    sql = "select distinct types from usgs.types;"
    cursor.execute(sql)
    existing_types_df = pd.DataFrame(data=cursor.fetchall(), columns=['exist_types'])
    cursor.close()

    # use types left join types_df
    if existing_types_df.empty:
        types_df.to_sql("types", con=engine, if_exists='append', index=False)
    else:
        types_df2 = pd.DataFrame(data=types_df, columns=["types"])
        types_df3 = pd.merge(types_df2, existing_types_df, how='left', left_on='types', right_on='exist_types')
        types_df4 = types_df3["types"][types_df3["types"].isna()]
        if not types_df4.empty:
            types_df4.to_sql("types", con=engine, if_exists='append', index=False)

    # sources_properties
    conn = engine.raw_connection()
    cursor1 = conn.cursor()
    cursor2 = conn.cursor()
    s_p_df1 = pd.DataFrame(sources_properties_df, columns=["sources", 'id_str'])

    # generate sources reference table
    cursor1.execute("select sid, sources from sources;")
    cursor2.execute("select pid, id from properties;")
    sources_ref = pd.DataFrame(data=cursor1.fetchall(), columns=['sid', 'sources_ref'])
    properties_ref = pd.DataFrame(data=cursor2.fetchall(), columns=['pid', 'id_str_ref'])
    s_p_df2 = pd.merge(s_p_df1, sources_ref, how='left', left_on='sources', right_on='sources_ref')
    s_p_df2 = pd.merge(s_p_df2, properties_ref, how='left', left_on='id_str', right_on='id_str_ref')
    s_p_df3 = s_p_df2[['sid', 'pid']]
    fullLoadMySQL(s_p_df3, "sources_properties", engine)
    cursor1.close()
    cursor2.close()

    # types_properties
    conn = engine.raw_connection()
    cursor = conn.cursor()
    cursor.execute("select tid, types from types;")
    types_ref = pd.DataFrame(data=cursor.fetchall(), columns=['tid', 'types'])
    t_p_df1 = pd.DataFrame(types_properties_df, columns=["types", 'pid'])
    t_p_df2 = pd.merge(t_p_df1, types_ref, how='left', left_on='types', right_on='types')
    t_p_df3 = t_p_df2[["tid", "pid"]]
    fullLoadMySQL(t_p_df3, "types_properties", engine)
    cursor.close()

    # related_events
    # after checked ids found that some of the related events id are not in the list
    # which would be not from earthquake. so decided using id_str as ref key
    fullLoadMySQL(related_event_df, "related_events", engine)

    # geometry
    fullLoadMySQL(geometry_df, "geometry", engine)

    start_datetime = pd.Timestamp(end_date).date()

# create FK and index
cursor = conn.cursor()
cursor.execute( "ALTER table properties DROP COLUMN sources;"
                "ALTER table properties DROP COLUMN types;"
                "ALTER table properties DROP COLUMN ids;"
                "ALTER TABLE sources_properties ADD CONSTRAINT FK_properties_sources_properties FOREIGN KEY (pid) REFERENCES properties(pid);"
                "ALTER TABLE sources_properties ADD CONSTRAINT FK_sources_sources_properties FOREIGN KEY (sid) REFERENCES sources(sid);"
                "ALTER TABLE types_properties ADD CONSTRAINT FK_properties_types_properties FOREIGN KEY (pid) REFERENCES properties(pid);"
                "ALTER TABLE types_properties ADD CONSTRAINT FK_types_types_properties FOREIGN KEY (tid) REFERENCES types(tid);"
                "ALTER TABLE related_events ADD CONSTRAINT FK_properties_related_events_original FOREIGN KEY (original_event_pid) REFERENCES properties(pid);"
                "CREATE UNIQUE INDEX idx_properties_id ON properties (id);"
               )
cursor.close()
conn.close()







