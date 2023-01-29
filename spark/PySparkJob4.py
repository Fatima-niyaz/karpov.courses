'''
Для дашборда с отображением выполненных рейсов требуется собрать таблицу на основе наших данных.
'''

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def process(spark, flights_path, airlines_path, airports_path, result_path):
    """
    Основной процесс задачи.

    :param spark: SparkSession
    :param flights_path: путь до датасета c рейсами
    :param airlines_path: путь до датасета c авиалиниями
    :param airports_path: путь до датасета c аэропортами
    :param result_path: путь с результатами преобразований
    """
    flights = spark.read.parquet(flights_path)
    airlines = spark.read.parquet(airlines_path)
    airports_origin = spark.read.parquet(airports_path)
    airports_destination = spark.read.parquet(airports_path)

    datamart = flights \
        .join(other=airlines, on=airlines['IATA_CODE']==flights['AIRLINE'], how='inner') \
        .join(other=airports_origin, on=airports_origin['IATA_CODE'] == F.col('ORIGIN_AIRPORT'), how='inner') \
        .join(other=airports_destination, on=airports_destination['IATA_CODE'] == F.col('DESTINATION_AIRPORT'), how='inner') \
        .select(airlines['AIRLINE'].alias('AIRLINE_NAME'),
                F.col('TAIL_NUMBER'),
                airports_origin['COUNTRY'].alias('ORIGIN_COUNTRY'),
                airports_origin['AIRPORT'].alias('ORIGIN_AIRPORT_NAME'),
                airports_origin['LATITUDE'].alias('ORIGIN_LATITUDE'),
                airports_origin['LONGITUDE'].alias('ORIGIN_LONGITUDE'),
                airports_destination['COUNTRY'].alias('DESTINATION_COUNTRY'),
                airports_destination['AIRPORT'].alias('DESTINATION_AIRPORT_NAME'),
                airports_destination['LATITUDE'].alias('DESTINATION_LATITUDE'),
                airports_destination['LONGITUDE'].alias('DECTINATION_LONGITUDE')
                )


    datamart.show(truncate=False)
    datamart.write.mode('overwrite').parquet(result_path)


def main(flights_path, airlines_path, airports_path, result_path):
    spark = _spark_session()
    process(spark, flights_path, airlines_path, airports_path, result_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkJob4').getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--flights_path', type=str, default='flights.parquet', help='Please set flights datasets path.')
    parser.add_argument('--airlines_path', type=str, default='airlines.parquet', help='Please set airlines datasets path.')
    parser.add_argument('--airports_path', type=str, default='airports.parquet', help='Please set airports datasets path.')
    parser.add_argument('--result_path', type=str, default='result', help='Please set result path.')
    args = parser.parse_args()
    flights_path = args.flights_path
    airlines_path = args.airlines_path
    airports_path = args.airports_path
    result_path = args.result_path
    main(flights_path, airlines_path, airports_path, result_path)
