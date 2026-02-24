import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node inputDataFromS3
inputDataFromS3_node1771901703961 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "escaper": "\\", "withHeader": True, "separator": ",", "multiLine": "true", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://dxif-bucket-test/test/csvdata"]}, transformation_ctx="inputDataFromS3_node1771901703961")

# Script generated for node SQL Query
SqlQuery47 = '''
select * from myDataSource
where flg = '1'
'''
SQLQuery_node1771909307109 = sparkSqlQuery(glueContext, query = SqlQuery47, mapping = {"myDataSource":inputDataFromS3_node1771901703961}, transformation_ctx = "SQLQuery_node1771909307109")

# Script generated for node Amazon S3
AmazonS3_node1771906263813 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1771909307109, connection_type="s3", format="csv", connection_options={"path": "s3://dxif-bucket-test/test/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1771906263813")

job.commit()