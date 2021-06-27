from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from utils import utilities as ut
import yaml
import os.path
from pyspark.sql.types import StructType, IntegerType, BooleanType,DoubleType

if __name__ == '__main__':
    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("DataFrames examples") \
        .config("spark.mongodb.input.uri", app_secret["mongodb_config"]["uri"])\
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    # Setup spark to use s3
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])
    tgt_list = app_conf["target_list"]
    # Check if passed from cmd line arg then override the above (e.g. source_list=OL,SB)
    for tgt in tgt_list:
        staging_path = "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["s3_conf"]["staging_dir"]
        tgt_conf = app_conf[tgt]
        s3_temp_path = "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/temp"
        if tgt == 'REGIS_DIM':
            print("\nCreating REGIS_DIM table")
            spark.read \
                .parquet(staging_path+ "/" + tgt_conf["source_data"])\
                .createOrReplaceTempView(tgt_conf["source_data"])

            regis_dim_df = spark.sql(tgt_conf["loadingQuery"])
            regis_dim_df.show()
            jdbc_url = ut.get_redshift_jdbc_url(app_secret)
            print(jdbc_url)
            ut.write_to_redshift(regis_dim_df.coalesce(1),
                                 jdbc_url,
                                 s3_temp_path,
                                 tgt_conf["tableName"])

        elif tgt == 'CHILD_DIM':
            print("\nCreating CHILD_DIM table")
            spark.read \
                .parquet(staging_path+ "/" + tgt_conf["source_data"])\
                .createOrReplaceTempView(tgt_conf["source_data"])

            child_dim_df = spark.sql(tgt_conf["loadingQuery"])
            child_dim_df.show()
            jdbc_url = ut.get_redshift_jdbc_url(app_secret)
            print(jdbc_url)
            ut.write_to_redshift(child_dim_df.coalesce(1),
                                 jdbc_url,
                                 s3_temp_path,
                                 tgt_conf["tableName"])

        elif tgt == 'RTL_TXN_FCT':
            src_list = tgt_conf["source_data"]
            print("\nCreating RTL_TXN_FCT table")
            #Read data from sources
            for src in src_list:
                spark.read \
                    .parquet(staging_path + "/" + src) \
                    .createOrReplaceTempView(src)

            jdbc_url = ut.get_redshift_jdbc_url(app_secret)
            print(jdbc_url)
            #Read the target table from redshift
            ut.read_from_redshift(spark,
                                  jdbc_url,
                                  s3_temp_path,
                                  "select * from {0}.{1} where ins_dt = current_date".format(app_conf["datamart_schema"],tgt_conf["target_src_table"]))\
                .createOrReplaceTempView(tgt_conf["target_src_table"])

            rtl_txn_fct_df = spark.sql(tgt_conf["loadingQuery"])
            rtl_txn_fct_df.show()

            ut.write_to_redshift(rtl_txn_fct_df.coalesce(1),
                                 jdbc_url,
                                 s3_temp_path,
                                 tgt_conf["tableName"])



# spark-submit --jars "https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/1.2.36.1060/RedshiftJDBC42-no-awssdk-1.2.36.1060.jar" --packages "io.github.spark-redshift-community:spark-redshift_2.11:4.0.1,org.apache.spark:spark-avro_2.11:2.4.2,org.apache.hadoop:hadoop-aws:2.7.4" com/dsm/target_data_loading.py
