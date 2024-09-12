"""
spark.py
~~~~~~~~

Module containing helper function for use with Apache Spark
"""

import __main__

from os import environ, listdir, path
import json
from pyspark import SparkFiles
from pyspark.sql import SparkSession

from dependencies import logging


def start_spark(app_name='my_spark_app', master='local[*]', jar_packages=[],
                files=[], spark_config={}):
    """Start Spark session, get Spark logger and load config files.

    Start a Spark session on the worker node and register the Spark
    application with the cluster. Note, that only the app_name argument
    will apply when this is called from a script sent to spark-submit.
    All other arguments exist solely for testing the script from within
    an interactive Python console.

    在工作节点上启动 Spark 会话，并将 Spark 应用注册到集群中。请注意，当通过 
    spark-submit 运行脚本时，只有 app_name 参数会生效。其他参数仅用于在交互式 Python 
    控制台中测试脚本。

    This function also looks for a file ending in 'config.json' that
    can be sent with the Spark job. If it is found, it is opened,
    the contents parsed (assuming it contains valid JSON for the ETL job
    configuration) into a dict of ETL job configuration parameters,
    which are returned as the last element in the tuple returned by
    this function. If the file cannot be found then the return tuple
    only contains the Spark session and Spark logger objects and None
    for config.

    此函数还会查找以 'config.json' 结尾的文件，该文件可以与 Spark 作业一起发送。
    如果找到该文件，函数会打开它，并假设文件中包含有效的 JSON 配置，用于 ETL 作业的配置参数。
    文件内容将被解析为一个字典形式，并作为此函数返回的元组中的最后一个元素返回。如果找不到此文件，
    则返回的元组仅包含 Spark 会话和 Spark 日志记录对象，配置文件部分为 None。

    The function checks the enclosing environment to see if it is being
    run from inside an interactive console session or from an
    environment which has a `DEBUG` environment variable set (e.g.
    setting `DEBUG=1` as an environment variable as part of a debug
    configuration within an IDE such as Visual Studio Code or PyCharm.
    In this scenario, the function uses all available function arguments
    to start a PySpark driver from the local PySpark package as opposed
    to using the spark-submit and Spark cluster defaults. This will also
    use local module imports, as opposed to those in the zip archive
    sent to spark via the --py-files flag in spark-submit.

    该函数还会检查所在的环境，以确定它是否在交互式控制台会话中运行，
    或是在设置了 DEBUG 环境变量的环境中运行（例如，在 IDE（如 Visual Studio Code 或 
    PyCharm）的调试配置中，将 DEBUG=1 设置为环境变量）。在这种情况下，函数将使用所有可用的函数参数，
    从本地 PySpark 包启动一个 PySpark 驱动程序，而不是使用 spark-submit 和 Spark 集群的默认配置。
    同时，该函数将使用本地模块导入，而不是通过 spark-submit 发送的 --py-files 标志中的 zip 文件中的模块。
    
    :param app_name: Name of Spark app.
    :param master: Cluster connection details (defaults to local[*]).
    :param jar_packages: List of Spark JAR package names.
    :param files: List of files to send to Spark cluster (master and
        workers).
    :param spark_config: Dictionary of config key-value pairs.
    :return: A tuple of references to the Spark session, logger and
        config dict (only if available).
    """

    # detect execution environment
    flag_repl = not(hasattr(__main__, '__file__'))
    flag_debug = 'DEBUG' in environ.keys()

    if not (flag_repl or flag_debug):
        # get Spark session factory
        spark_builder = (
            SparkSession
            .builder
            .appName(app_name))
    else:
        # get Spark session factory
        spark_builder = (
            SparkSession
            .builder
            .master(master)
            .appName(app_name))

        # create Spark JAR packages string
        spark_jars_packages = ','.join(list(jar_packages))
        spark_builder.config('spark.jars.packages', spark_jars_packages)

        spark_files = ','.join(list(files))
        spark_builder.config('spark.files', spark_files)

        # add other config params
        for key, val in spark_config.items():
            spark_builder.config(key, val)

    # create session and retrieve Spark logger object
    spark_sess = spark_builder.getOrCreate()
    spark_logger = logging.Log4j(spark_sess)

    # get config file if sent to cluster with --files
    spark_files_dir = SparkFiles.getRootDirectory()
    config_files = [filename
                    for filename in listdir(spark_files_dir)
                    if filename.endswith('config.json')]

    if config_files:
        path_to_config_file = path.join(spark_files_dir, config_files[0])
        with open(path_to_config_file, 'r') as config_file:
            config_dict = json.load(config_file)
        spark_logger.warn('loaded config from ' + config_files[0])
    else:
        spark_logger.warn('no config file found')
        config_dict = None

    return spark_sess, spark_logger, config_dict
