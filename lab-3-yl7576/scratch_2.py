#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Starter Pyspark Script for students to complete for their Lab 3 Assignment.
Usage:
    $ spark-submit lab_3_starter_code.py <student_netID>
'''
#Use getpass to obtain user netID
import getpass

# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession

import pyspark.sql.functions as f

def main(spark):
    '''Main routine for Lab Solutions
    Parameters
    ----------
    spark : SparkSession object
    netID : string, netID of student to find files in HDFS
    '''
    print('Lab 3 Example dataframe loading and SQL query')

    # Load the boats.txt and sailors.json data into DataFrame
    boats = spark.read.csv('/Users/yuhanliu/Desktop/1004/lab-3-yl7576/boats.txt')
    sailors = spark.read.json('/Users/yuhanliu/Desktop/1004/lab-3-yl7576/sailors.json')

    print('Printing boats inferred schema')
    boats.printSchema()
    print('Printing sailors inferred schema')
    sailors.printSchema()
    #Why does sailors already have a specified schema?

    print('Reading boats.txt and specifying schema')
    boats = spark.read.csv('boats.txt', schema='bid INT, bname STRING, color STRING')

    print('Printing boats with specified schema')
    boats.printSchema()

    # Give the dataframe a temporary view so we can run SQL queries
    boats.createOrReplaceTempView('boats')
    sailors.createOrReplaceTempView('sailors')
    # Construct a query
    print('Example 1: Executing SELECT count(*) FROM boats with SparkSQL')
    query = spark.sql('SELECT count(*) FROM boats')

    # Print the results to the console
    query.show()

    #####--------------YOUR CODE STARTS HERE--------------#####

    #make sure to load reserves.json, artist_term.csv, and tracks.csv
    #For the CSVs, make sure to specify a schema!

    artist_term = spark.read.csv('/Users/yuhanliu/Desktop/1004/lab-3-yl7576/artist_term.csv', schema='artistID STRING,term STRING')
    tracks = spark.read.csv('/Users/yuhanliu/Desktop/1004/lab-3-yl7576/tracks.csv', schema='trackID STRING, title STRING, release STRING, year INT, duration FLOAT, artistID STRING')
    reserves = spark.read.json('/Users/yuhanliu/Desktop/1004/lab-3-yl7576/reserves.json')
    print('Printing artist_term with specified schema')
    artist_term.printSchema()
    print('Printing tracks with specified schema')
    tracks.printSchema()
    print('Printing reserves with specified schema')
    reserves.printSchema()

    #question_1
    print('Question 1 Output')
    q1_query = spark.sql('SELECT sid, sname, age FROM sailors WHERE rating > 6')
    q1_query.show()

    #question_2
    print('Question 2 Output')
    reserves.filter(reserves.bid != 101)\
            .groupBy(reserves.sid)\
            .agg(f.count(reserves.bid))\
            .show()

    #question_3
    print('Question 3 Output')
    sailors.join(reserves, ['sid'], 'inner')\
           .groupBy(sailors.sid, sailors.sname)\
           .agg(f.countDistinct(reserves.bid))\
           .show()

    #question_4
    print('Question 4 Output')
    tracks.join(artist_term, ['artist_id'], 'left')\
          .groupBy(artist_term.tag)\
          .agg(f.min(tracks.year), f.avg(tracks.duration).alias('avg_duration'), f.count(artist_term.artist_id))\
          .orderBy(f.col('avg_duration').desc())\
          .first('avg_duration')\
          .show()

    #question_5
    print('Question 5 Output: top 10')
    artist_term.join(tracks, ['artistID'], 'left')\
               .groupBy(artist_term.term)\
               .agg(f.countDistinct(tracks.trackID).alias('num_track'))\
               .orderBy(f.col('num_track').desc())\
               .first('num_track')\
               .show()

    print('Question 5 Output: bottom 10')
    artist_term.join(tracks, ['artistID'], 'left')\
               .groupBy(artist_term.term)\
               .agg(f.countDistinct(tracks.trackID).alias('num_track'))\
               .orderBy(f.col('num_track').desc())\
               .last('num_track')\
               .show()



# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part1').getOrCreate()

    # Get user netID from the command line
    netID = getpass.getuser()

    # Call our main routine
    main(spark)
