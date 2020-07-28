import sys
from argparse import ArgumentParser
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, StructField
from pyspark.sql.functions import count, col
import os

import configparser


def build_argparser():
    parser = ArgumentParser()
    parser.add_argument( "-i", "--input", help="S3 input path.", type=str)
    parser.add_argument( "-o", "--output", help="S3 output path.", type=str)
    parser.add_argument( "-p", "--profile", help="AWS credentials profile", default='default', type=str)
    return parser


def parse_aws_creds(aws_profile):
    aws_profile = aws_profile
    config = configparser.ConfigParser()
    config.read(os.path.expanduser("~/.aws/credentials"))
    access_id = config.get(aws_profile, "aws_access_key_id") 
    access_key = config.get(aws_profile, "aws_secret_access_key")     
    return access_id, access_key


def read_input(spark, input_path):
    schema = StructType([StructField("key", IntegerType(), True), StructField("value", IntegerType(), True)])
    
    df = spark.createDataFrame([], schema=schema)
    delimiter_map = {"*.csv":",","*.tsv":"\t"}

    for file_type in delimiter_map:
        file_path = os.path.join(input_path,file_type)
        new_df = spark.read.option("header", "true").option("delimiter", delimiter_map[file_type]).schema(schema).csv(file_path)
        new_df = new_df.fillna({'value':0})
        df = df.union(new_df)
    return df


def calculate_odd_key(df):
    grouped_df = df.groupby("key", "value").agg((count("*")%2).alias("is_odd"))
    grouped_df = grouped_df.where(col('is_odd') == 1).select(col("key"),col("value"))
    return grouped_df


def write_output(spark, output_df, output_path):
    
    spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", output_path)

    (output_df.write.option("delimiter", "\t")
                    .format("com.databricks.spark.csv")
                    .option("header","true")
                    .mode("Overwrite")
                    .save(output_path))

    return None


def main(input_path, output_path, aws_profile):
    access_id, access_key = parse_aws_creds(aws_profile)

    os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell"

    spark = SparkSession.builder.appName("convex").config('spark.sql.codegen.wholeStage', False).getOrCreate()
    spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_id)
    spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", access_key)

    df = read_input(spark, input_path)
    output_df = calculate_odd_key(df)
    output_df.show()
    write_output(spark, output_df, output_path)

    return None




if __name__ == '__main__':
    args = build_argparser().parse_args()
    main(args.input, args.output, args.profile)
    
    
