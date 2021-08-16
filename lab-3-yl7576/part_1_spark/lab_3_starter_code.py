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

def main(spark, netID):
    '''Main routine for Lab Solutions
    Parameters
    ----------
    spark : SparkSession object
    netID : string, netID of student to find files in HDFS
    '''
    print('Lab 3 Example dataframe loading and SQL query')

    # Load the boats.txt and sailors.json data into DataFrame
    boats = spark.read.csv(f'hdfs:/user/{netID}/boats.txt')
    sailors = spark.read.json(f'hdfs:/user/{netID}/sailors.json')

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

    artist_term = spark.read.csv(f'hdfs:/user/{netID}/artist_term.csv', schema='artistID STRING,term STRING')
    tracks = spark.read.csv(f'hdfs:/user/{netID}/tracks.csv', schema='trackID STRING, title STRING, release STRING, year INT, duration FLOAT, artistID STRING')
    reserves = spark.read.json(f'hdfs:/user/{netID}/reserves.json')
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
    q2 = reserves.filter(reserves.bid != 101)\
            .groupBy(reserves.sid)\
            .agg(f.count(reserves.bid))
    q2.show()

    #question_3
    print('Question 3 Output')
    q3 = sailors.join(reserves, ['sid'], 'inner')\
           .groupBy(sailors.sid, sailors.sname)\
           .agg(f.countDistinct(reserves.bid))
    q3.show()

    #question_4
    print('Question 4 Output')
    q4 = tracks.join(artist_term, ['artistID'], 'left')\
          .groupBy(artist_term.term)\
          .agg(f.min(tracks.year), f.avg(tracks.duration).alias('avg_duration'), f.count(tracks.artistID))\
          .orderBy(f.col('avg_duration').desc())
    q4.show(10)

    #question_5
    print('Question 5 Output: top 10')
    q5_top = artist_term.join(tracks, ['artistID'], 'left')\
                        .groupBy(artist_term.term)\
                        .agg(f.countDistinct(tracks.trackID).alias('num_track'))\
                        .orderBy(f.col('num_track').desc())
    q5_top.show(10)
    print('Question 5 Output: bottom 10')
    q5_bottom = artist_term.join(tracks, ['artistID'], 'left')\
                           .groupBy(artist_term.term)\
                           .agg(f.countDistinct(tracks.trackID).alias('num_track'))\
                           .orderBy('num_track')
    q5_bottom.show(10)


# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part1').getOrCreate()

    # Get user netID from the command line
    netID = getpass.getuser()

    # Call our main routine
    main(spark, 'yl7576')

