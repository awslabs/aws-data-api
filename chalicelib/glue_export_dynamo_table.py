# Simple Glue Export of table from DynamoDB to S3
EXPORT_ARG_TABLE_NAME = "TableName"
EXPORT_ARG_READ_PCT = "ReadPercentage"
EXPORT_ARG_PREFIX = "OutputPrefix"
EXPORT_ARG_FORMAT = "OutputFormat"

# signature of a glue python job is to run from __main__
if __name__ == '__main__':
    import sys
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext, SparkConf
    from awsglue.context import GlueContext

    # setup gzip compression
    # TODO figure out why this isn't working
    conf = SparkConf()
    conf.set("spark.hadoop.mapred.output.compress", "true")
    conf.set("spark.hadoop.mapred.output.compression.codec", "true")
    conf.set("spark.hadoop.mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
    conf.set("spark.hadoop.mapred.output.compression.type", "BLOCK")

    # import arguments from the Glue invocation interface
    args = getResolvedOptions(
        sys.argv,
        [
            'JOB_NAME',
            EXPORT_ARG_TABLE_NAME,
            EXPORT_ARG_READ_PCT,
            EXPORT_ARG_PREFIX,
            EXPORT_ARG_FORMAT
        ]
    )

    table_name = args[EXPORT_ARG_TABLE_NAME]
    read = args[EXPORT_ARG_READ_PCT]
    output_prefix = args[EXPORT_ARG_PREFIX]
    fmt = args[EXPORT_ARG_FORMAT]

    sc = SparkContext()

    # Create a glue context that can read from DynamoDB
    glueContext = GlueContext(sc)
    table = glueContext.create_dynamic_frame.from_options(
        "dynamodb",
        connection_options={
            "dynamodb.input.tableName": table_name,
            "dynamodb.throughput.read.percent": read
        }
    )

    # convert to DataFrame so we can support overwrite, which is the output approach for data api exports
    table.toDF().write.mode("overwrite").format(fmt).save(
        output_prefix)
