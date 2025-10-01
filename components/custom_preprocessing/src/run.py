# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Entry script for Model Data Collector Data Window Component."""

import argparse
import pandas as pd
import mltable
import tempfile
import os
from azureml.fsspec import AzureMachineLearningFileSystem
from datetime import datetime
from dateutil import parser
from pyspark.sql import SparkSession
import logging
from pathlib import Path
import sys
# Set up logger
logger = logging.getLogger("model_monitoring")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

def init_spark():
    """Get or create spark session."""
    spark = SparkSession.builder.appName("AccessParquetFiles").getOrCreate()
    return spark


def read_mltable_in_spark(mltable_path: str):
    """Read mltable in spark."""
    spark = init_spark()
    df = spark.read.mltable(mltable_path)
    return df


def save_spark_df_as_mltable(metrics_df, folder_path: str):
    """Save spark dataframe as mltable."""
    metrics_df.write.option("output_format", "parquet").option(
        "overwrite", True
    ).mltable(folder_path)


# def preprocess(
#     data_window_start: str,
#     data_window_end: str,
#     input_data: str,
#     preprocessed_input_data: str,
# ):
#     """Extract data based on window size provided and preprocess it into MLTable.

#     Args:
#         production_data: The data asset on which the date filter is applied.
#         monitor_current_time: The current time of the window (inclusive).
#         window_size_in_days: Number of days from current time to start time window (exclusive).
#         preprocessed_data: The mltable path pointing to location where the outputted mltable will be written to.
#     """
#     format_data = "%Y-%m-%d %H:%M:%S"
#     start_datetime = parser.parse(data_window_start)
#     start_datetime = datetime.strptime(
#         str(start_datetime.strftime(format_data)), format_data
#     )

#     # TODO Validate window size
#     end_datetime = parser.parse(data_window_end)
#     end_datetime = datetime.strptime(
#         str(end_datetime.strftime(format_data)), format_data
#     )

#     # Create mltable, create column with partitionFormat
#     # Extract partition format
#     # table = mltable.from_json_lines_files(
#     #     paths=[{"pattern": f"{input_data}**/*.jsonl"}]
#     # )
#     table = mltable.from_parquet_files(
#         paths=[{"pattern":f"{input_data}**/*.parquet"}]
#     )
#     # Uppercase HH for hour info
#     partitionFormat = "{PartitionDate:yyyy/MM/dd/HH}/{fileName}.jsonl"
#     table = table.extract_columns_from_partition_format(partitionFormat)

#     # Filter on partitionFormat based on user data window
#     filterStr = f"PartitionDate >= datetime({start_datetime.year}, {start_datetime.month}, {start_datetime.day}, {start_datetime.hour}) and PartitionDate <= datetime({end_datetime.year}, {end_datetime.month}, {end_datetime.day}, {end_datetime.hour})"  # noqa
#     table = table.filter(filterStr)

#     # Data column is a list of objects, convert it into string because spark.read_json cannot read object
#     table = table.convert_column_types({"data": mltable.DataType.to_string()})

#     # Use NamedTemporaryFile to create a secure temp file
#     with tempfile.NamedTemporaryFile(delete=False) as temp_file:
#         save_path = temp_file.name
#         table.save(save_path)

#     # Save preprocessed_data MLTable to temp location
#     des_path = preprocessed_input_data + "temp"
#     fs = AzureMachineLearningFileSystem(des_path)
#     print("MLTable path:", des_path)
#     # TODO: Evaluate if we need to overwrite
#     fs.upload(
#         lpath=save_path,
#         rpath="",
#         **{"overwrite": "MERGE_WITH_OVERWRITE"},
#         recursive=True,
#     )

#     # Read mltable from preprocessed_data
#     df = read_mltable_in_spark(mltable_path=des_path)

#     if df.count() == 0:
#         raise Exception(
#             "The window for this current run contains no data. "
#             + "Please visit aka.ms/mlmonitoringhelp for more information."
#         )

#     # Output MLTable
#     first_data_row = df.select("data").rdd.map(lambda x: x).first()

#     spark = init_spark()
#     data_as_df = spark.createDataFrame(pd.read_json(first_data_row["data"]))

#     def tranform_df_function(iterator):
#         for df in iterator:
#             yield pd.concat(pd.read_json(entry) for entry in df["data"])

#     transformed_df = df.select("data").mapInPandas(
#         tranform_df_function, schema=data_as_df.schema
#     )

#     save_spark_df_as_mltable(transformed_df, preprocessed_input_data)
# Replace all prints in preprocess with logger
def preprocess(
    data_window_start: str,
    data_window_end: str,
    input_data: str,
    preprocessed_input_data: str,
):
    """
    Extract data based on window size provided and preprocess it into MLTable.

    Args:
        data_window_start: Start of the time window (inclusive).
        data_window_end: End of the time window (inclusive).
        input_data: Path to input Parquet files.
        preprocessed_input_data: Path where the output MLTable will be written.
    """
    # format_data = "%Y-%m-%d %H:%M:%S"
    format_data = "%Y-%m-%dT%H:%M:%S"
    start_datetime = datetime.strptime(parser.parse(data_window_start).strftime(format_data), format_data)
    end_datetime = datetime.strptime(parser.parse(data_window_end).strftime(format_data), format_data)
    logger.info(f"input data is: {input_data}")
    table = mltable.from_parquet_files(
        paths=[{"pattern": f"{input_data}**/*.parquet"}]
    )
    logger.info(table.show())
    # filter_str = (
    #     f"TIMESTAMP >= datetime({start_datetime.year}, {start_datetime.month}, {start_datetime.day}) "
    #     f"and TIMESTAMP <= datetime({end_datetime.year}, {end_datetime.month}, {end_datetime.day})"
    # )
    filter_str = (
    f"TIMESTAMP >= '{start_datetime.strftime(format_data)}' "
    f"and TIMESTAMP <= '{end_datetime.strftime(format_data)}'"
    )
    table = table.filter(filter_str)
    print(table.show())
    # table.save(preprocessed_input_data)
    table.save(Path(preprocessed_input_data))
    
    des_path = preprocessed_input_data
    fs = AzureMachineLearningFileSystem(des_path)
    print("MLTable path:", des_path)
    fs.upload(
        lpath=preprocessed_input_data,
        rpath="",
        **{"overwrite": "MERGE_WITH_OVERWRITE"},
        recursive=True,
    )
    # df = read_mltable_in_spark(mltable_path=des_path)
    # save_spark_df_as_mltable(df, preprocessed_input_data)

    # fs = AzureMachineLearningFileSystem(preprocessed_input_data)
    # logger.info(f"MLTable path: {des_path}")
    # fs.upload(lpath=preprocessed_input_data, rpath="", recursive=True, **{"overwrite": "MERGE_WITH_OVERWRITE"})


    # with tempfile.TemporaryDirectory() as temp_dir:
    #     save_path = temp_dir
    #     logger.info(f"Temporary directory for MLTable: {save_path}")
    #     table.save(temp_dir)
    #     logger.info(f"Saved MLTable to folder: {temp_dir}")
    #     logger.info(f"Contents: {os.listdir(temp_dir)}")
    #     des_path = os.path.join(preprocessed_input_data.rstrip("/"), f"{temp_dir.split('/')[-1]}/")
    #     fs = AzureMachineLearningFileSystem(des_path)
    #     logger.info(f"MLTable path: {des_path}")
    #     fs.upload(lpath=temp_dir, rpath="", recursive=True, **{"overwrite": "MERGE_WITH_OVERWRITE"})
# def preprocess(
#     data_window_start: str,
#     data_window_end: str,
#     input_data: str,
#     preprocessed_input_data: str,
# ):
#     """
#     Extract data based on window size provided and preprocess it into MLTable.

#     Args:
#         data_window_start: Start of the time window (inclusive).
#         data_window_end: End of the time window (inclusive).
#         input_data: Path to input Parquet files.
#         preprocessed_input_data: Path where the output MLTable will be written.
#     """
#     # Parse start and end datetimes
#     format_data = "%Y-%m-%d %H:%M:%S"
#     start_datetime = datetime.strptime(parser.parse(data_window_start).strftime(format_data), format_data)
#     end_datetime = datetime.strptime(parser.parse(data_window_end).strftime(format_data), format_data)
#     print(f"input data is: {input_data}")
#     # Load Parquet files into MLTable
#     table = mltable.from_parquet_files(
#         paths=[{"pattern": f"{input_data}**/*.parquet"}]
#     )
#     print(table.show())
#     # Filter rows based on TIMESTAMP column instead of partition format
#     filter_str = (
#         f"TIMESTAMP >= datetime({start_datetime.year}, {start_datetime.month}, {start_datetime.day}) "
#         f"and TIMESTAMP <= datetime({end_datetime.year}, {end_datetime.month}, {end_datetime.day})"
#     )
#     table = table.filter(filter_str)

#     # Save filtered MLTable to a temporary location
#     # with tempfile.NamedTemporaryFile(delete=False) as temp_file:
#     #     save_path = temp_file.name
#     #     table.save(save_path)
    
#     with tempfile.TemporaryDirectory() as temp_dir:
#         # Save the MLTable to the directory
#         save_path = temp_dir
#         print(save_path)
#         table.save(temp_dir)

#         # This will create a file: temp_dir/MLTable
#         print("Saved MLTable to folder:", temp_dir)
#         print("Contents:", os.listdir(temp_dir))

#         # Upload MLTable spec folder to AML datastore
#         # des_path = os.path.join(preprocessed_input_data.rstrip("/"), "temp/")  # ensure folder path
#         des_path = os.path.join(preprocessed_input_data.rstrip("/"), f"{temp_dir.split('/')[-1]}/")
#         fs = AzureMachineLearningFileSystem(des_path)
#         print("MLTable path:", des_path)
#         fs.upload(lpath=temp_dir, rpath="", recursive=True, **{"overwrite": "MERGE_WITH_OVERWRITE"})


#     # # Upload MLTable to Azure ML datastore
#     # des_path = preprocessed_input_data + "temp"
#     # fs = AzureMachineLearningFileSystem(des_path)
#     # print("MLTable path:", des_path)
#     # fs.upload(lpath=save_path, rpath="", **{"overwrite": "MERGE_WITH_OVERWRITE"}, recursive=True)

#         # # Read MLTable into Spark
#         # spark = SparkSession.builder.getOrCreate()
#         # df = spark.read.format("mltable").load(des_path)

#         # if df.count() == 0:
#         #     raise Exception(
#         #         "The window for this current run contains no data. "
#         #         + "Please visit aka.ms/mlmonitoringhelp for more information."
#         #     )

#         # # Convert TIMESTAMP to proper type if needed
#         # df = df.withColumn("TIMESTAMP", df["TIMESTAMP"].cast("timestamp"))

#         # # Save final Spark DataFrame as MLTable
#         # df.write.format("mltable").mode("overwrite").save(preprocessed_input_data)


def run():
    """Compute data window and preprocess data from MDC."""
    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_window_start", type=str)
    parser.add_argument("--data_window_end", type=str)
    parser.add_argument("--input_data", type=str)
    parser.add_argument("--preprocessed_input_data", type=str)
    args = parser.parse_args()
    print("We are inside run")
    logger.info("We are inside run")
    preprocess(
        args.data_window_start,
        args.data_window_end,
        args.input_data,
        args.preprocessed_input_data,
    )


if __name__ == "__main__":
    print("This is hte main function calling...")
    logger.info("This is the main function calling inside logger..")
    run()