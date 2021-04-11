from abc import ABC, abstractmethod
from typing import List, Dict, Any, Union

from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import regexp_replace, lower, trim, col, when
from pyspark.sql.types import (IntegerType,
                               StructType,
                               StructField,
                               StringType,
                               LongType,
                               BooleanType,
                               DateType,
                               FloatType)


class Entity(ABC):

    def __init__(self, type_field: str, table_name: str):
        self.type_field = type_field
        self.table_name = table_name

    @abstractmethod
    def schema(self) -> StructType: pass

    def read(self, spark: SparkSession, sc: SparkContext, objs: List[Union[str, Dict[str, Any]]]) -> DataFrame:
        df = (spark.read
              .schema(self.schema())
              .option("mode", "DROPMALFORMED")
              .json(sc.parallelize(objs), multiLine=True))
        return (df
                .select('type', 'data.*')
                .filter(df.type == self.type_field))

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame: pass

    def upload(self, df: DataFrame, url: str, properties: Dict[str, str], table_name=None, mode='append') -> None:
        table_name = table_name if table_name else self.table_name
        (df.drop("type")
         .repartition(8)
         .write.jdbc(url, table_name, mode, properties))

    @staticmethod
    @abstractmethod
    def sql() -> str: pass


class Song(Entity):
    def __init__(self, type_field='song', table_name='songs'):
        super().__init__(type_field, table_name)

    def schema(self) -> StructType:
        return StructType([
            StructField('type', StringType(), nullable=False),
            StructField('data', StructType([
                StructField('artist_name', StringType()),
                StructField('title', StringType()),
                StructField('year', IntegerType()),
                StructField('release', StringType()),
            ]))
        ])

    def transform(self, df: DataFrame) -> DataFrame:
        return df

    @staticmethod
    def sql() -> str:
        return """
        CREATE TABLE IF NOT EXISTS songs (
            artist_name VARCHAR ( 300 ),
            title VARCHAR ( 300 ), 
            year INT, 
            release VARCHAR ( 300 ), 
            ingestion_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP 
        )
    """


class Movie(Entity):
    def __init__(self, type_field='movie', table_name='movies'):
        super().__init__(type_field, table_name)

    def schema(self) -> StructType:
        return StructType([
            StructField('type', StringType(), nullable=False),
            StructField('data', StructType([
                StructField('original_title', StringType()),
                StructField('original_language', StringType()),
                StructField('budget', LongType()),
                StructField('is_adult', BooleanType()),
                StructField('release_date', DateType()),
            ]))
        ])

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Fill original_title_normalized with value of original_title
        where any non-letter and non-number characters are removed
        and spaces replaced with underscore _

        Example: Star Wars: The Force Awakens -> star_wars_the_force_awakens

        :param df: DataFrame
        :return: DataFrame
        """
        return df.withColumn('original_title_normalized',
                             regexp_replace(regexp_replace(lower(trim(
                                 col('original_title'))),
                                 '\s+', '_'), '\W', ''))

    @staticmethod
    def sql() -> str:
        return """
        CREATE TABLE IF NOT EXISTS movies (
            original_title VARCHAR ( 300 ),
            original_language VARCHAR ( 50 ), 
            budget BIGINT, 
            is_adult BOOLEAN, 
            release_date DATE, 
            original_title_normalized VARCHAR ( 300 )  
        )
    """


class App(Entity):
    def __init__(self, type_field='app', table_name='apps'):
        super().__init__(type_field, table_name)

    def schema(self) -> StructType:
        return StructType([
            StructField('type', StringType(), nullable=False),
            StructField('data', StructType([
                StructField('name', StringType()),
                StructField('genre', StringType()),
                StructField('rating', FloatType()),
                StructField('version', StringType()),
                StructField('size_bytes', LongType()),
            ]))
        ])

    def transform(self, df: DataFrame) -> DataFrame:
        return df.withColumn('is_awesome',
                             when(col('rating') >= 4, True)
                             .otherwise(False))

    @staticmethod
    def sql() -> str:
        return """
        CREATE TABLE IF NOT EXISTS apps (
            name VARCHAR ( 300 ),
            genre VARCHAR ( 50 ), 
            rating FLOAT, 
            version VARCHAR ( 50 ), 
            size_bytes BIGINT, 
            is_awesome BOOLEAN  
        )
    """

