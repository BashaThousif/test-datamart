
def get_redshift_jdbc_url(redshift_config: dict):
    host = redshift_config["redshift_conf"]["host"]
    port = redshift_config["redshift_conf"]["port"]
    database = redshift_config["redshift_conf"]["database"]
    username = redshift_config["redshift_conf"]["username"]
    password = redshift_config["redshift_conf"]["password"]
    return "jdbc:redshift://{}:{}/{}?user={}&password={}".format(host, port, database, username, password)


def get_mysql_jdbc_url(mysql_config: dict):
    host = mysql_config["mysql_conf"]["hostname"]
    port = mysql_config["mysql_conf"]["port"]
    database = mysql_config["mysql_conf"]["database"]
    return "jdbc:mysql://{}:{}/{}?autoReconnect=true&useSSL=false".format(host, port, database)

def write_to_s3(df, path):
    print('Writing data to', path)
    df.write \
        .mode("append") \
        .partitionBy("ins_dt") \
        .parquet(path)

def read_from_redshift(spark,jdbc_url,s3_temp_path,query):
    return spark.read \
                .format("io.github.spark_redshift_community.spark.redshift") \
                .option("url", jdbc_url) \
                .option("query", query) \
                .option("forward_spark_s3_credentials", "true") \
                .option("tempdir", s3_temp_path) \
                .load()

def write_to_redshift(df,jdbc_url,s3_temp_path,table_name):
    df.write \
      .format("io.github.spark_redshift_community.spark.redshift") \
      .option("url", jdbc_url) \
      .option("dbtable", table_name) \
      .option("forward_spark_s3_credentials", "true") \
      .option("tempdir", s3_temp_path) \
      .mode("append") \
      .save()