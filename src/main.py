import argparse
from pathlib import Path
from typing import Dict, Any, List, Union

import psycopg2
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

from src.cacher import Cache
from src.db_connector import create_table, create_db
from src.entities import App, Song, Movie, Entity
from src.reader import BucketReader

import logging
import logging.config
import yaml


def get_kwargs() -> Dict[str, Any]:
    # Construct the argument parser
    ap = argparse.ArgumentParser()
    # Add the arguments to the parser
    # Source args
    ap.add_argument("-b", "--bucket", required=True, type=str, help="Bucket name")
    ap.add_argument("-k", "--key", required=True, type=str, help="Files list object key")
    ap.add_argument("-c", "--cache", default='.files_cache', type=Path, help="Files list cache")
    # Spark args
    ap.add_argument("-a", "--app", default='Json S3 Parser', type=str, help="SparkConf setAppName")
    ap.add_argument("-m", "--master", default='local[1]', type=str, help="SparkConf setMaster")
    # Postgres args
    ap.add_argument("-d", "--database", default='macpaw_de', type=str, help="Postgres db name")
    ap.add_argument("-u", "--user", default='macpaw_de', type=str, help="Postgres db user")
    ap.add_argument("-dh", "--host", default='localhost', type=str, help="Postgres db host")
    ap.add_argument("-p", "--password", required=True, type=str, help="Postgres db password")

    kwargs = vars(ap.parse_args())
    return kwargs


def session(app_name: str, master: str) -> SparkSession:
    conf = (SparkConf().setAppName(app_name).setMaster(master)
            .set('spark.jars', 'postgresql-42.2.10.jre6.jar'))
    # Create Spark session
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    return spark


def extract_transform_load(entity: Entity,
                           spark: SparkSession,
                           sc: SparkContext,
                           objs: List[Union[str, Dict[str, Any]]],
                           url: str,
                           properties: Dict[str, str],
                           conn: psycopg2.extensions.connection,
                           cur: psycopg2.extensions.cursor) -> None:
    df = entity.read(spark, sc, objs)
    df = entity.transform(df)

    logger.debug(df.count())
    df.printSchema()
    df.show(truncate=True)

    # Create Table if not exists
    create_table(entity.sql(), conn, cur)
    # Update Table
    entity.upload(df, url, properties)


def run() -> None:
    # Read files by key and bucket
    br = BucketReader()
    files: List[str] = br.list_files(bucket=kwargs['bucket'], key=kwargs['key'])
    logger.debug(f'Files in bucket: {len(files)}')

    # Get only new files and cache filenames
    cache = Cache()
    files = cache.new_files(kwargs['cache'], files)
    logger.debug(f'New files to process: {len(files)}')

    # If no new files, end execution
    logger.debug(files[0] if files else 'No new files')
    if not files:
        return

    # Get all json files
    objs: List[str] = list(br.get_json(kwargs['bucket'], files))

    # Start Spark Session
    spark = session(kwargs['app'], kwargs['master'])
    sc = spark.sparkContext

    # Database Credentials
    url = f"jdbc:postgresql://{kwargs['host']}:5432/{kwargs['database']}"
    properties = {"user": kwargs['user'],
                  "password": kwargs['password'],
                  "driver": "org.postgresql.Driver"}
    conn_info = {
        'user': kwargs['user'],
        'password': kwargs['password'],
        'database': kwargs['database'],
        'host': kwargs['host']
    }

    # Create Database
    create_db(conn_info)

    # Connect to the database created
    connection = psycopg2.connect(**conn_info)
    cursor = connection.cursor()

    # Populate postgres tables with entity data
    app, song, movie = App(), Song(), Movie()

    for e in [app, song, movie]:
        extract_transform_load(e, spark, sc, objs,
                               url, properties,
                               connection, cursor)

    # Avoid memory leaks
    connection.close()
    cursor.close()

    # Stop Spark Session
    spark.stop()


if __name__ == '__main__':
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)

    logger = logging.getLogger(__name__)

    kwargs = get_kwargs()
    logger.debug(kwargs)

    run()

