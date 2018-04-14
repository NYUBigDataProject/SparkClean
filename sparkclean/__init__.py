from IPython.display import display, HTML
from pyspark.sql.session import SparkSession
from sparkclean.utilities import *
from sparkclean.df_transformer import DataFrameTransformer
from sparkclean.df_deduplicator import DataFrameDeduplicator
from sparkclean.df_outliers import OutlierDetector 
import os

# -*- coding: utf-8 -*-

try:
    get_ipython
    def print_html(html):
        display(HTML(html))

    print_html("<div>Starting or getting SparkSession and SparkContext.</div>")

    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    sc = spark.sparkContext
    Utilities().set_check_point_folder(os.getcwd(), "local")


    message = "<b><h2>SparkClean successfully imported. Have fun :).</h2></b>"

    print_html(
        message
    )
except Exception:
    print("Shell detected")
    print("Starting or getting SparkSession and SparkContext.")

    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    sc = spark.sparkContext
    Utilities().set_check_point_folder(os.getcwd(), "local")

    print("SparkClean successfully imported. Have fun :).")
    print("---------------------------------------------------------------")
    print("|                                                             |")
    print("|                                                             |")
    print("|                                                             |")
    print("|                        SparkClean                           |")
    print("|                                                             |")
    print("|                         V 0.1                               |")
    print("|                                                             |")
    print("---------------------------------------------------------------")

# module level doc-string
__doc__ = """
SparkClean, data cleaning library for pyspark.
"""