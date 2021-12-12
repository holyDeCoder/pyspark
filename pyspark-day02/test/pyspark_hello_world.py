import os
from pyspark import SparkConf, SparkContext
import time


if __name__ == '__main__':
    # 0. 设置系统环境变量
    os.environ['JAVA_HOME'] = 'F:/cheng/Java/jdk1.8.0_241'
    os.environ['HADOOP_HOME'] = 'F:/cheng/hadoop-3.3.0'
    os.environ['PYSPARK_PYTHON'] = 'C:/ProgramData/Anaconda3/python.exe'
    os.environ['PYSPARK_DRIVER_PYTHON'] = 'C:/ProgramData/Anaconda3/python.exe'

    spark_conf = SparkConf().setAppName("TestSparkText").setMaster("local[2]")
    sc = SparkContext(conf=spark_conf)
    print(sc)

    sc.stop()



