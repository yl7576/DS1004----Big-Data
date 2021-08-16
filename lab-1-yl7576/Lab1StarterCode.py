#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# USAGE:
# python Lab1StarterCode.py Sample_Song_Dataset.db

import sys
import sqlite3


# The database file should be given as the first argument on the command line
# Please do not hard code the database file!
db_file = sys.argv[1]
import time
# We connect to the database using 
with sqlite3.connect(db_file) as conn:
    # We use a "cursor" to mark our place in the database.
    # We could use multiple cursors to keep track of multiple
    # queries simultaneously.
    cursor = conn.cursor()
    #cursor.execute('DROP INDEX artid_ind')
    #cursor.execute('DROP INDEX artid_ind2')
    #cursor.execute('DROP INDEX artid_ind3')

    # This query counts the number of tracks from the year 1998
    year = ('1998',)
    cursor.execute('SELECT count(*) FROM tracks WHERE year=?', year)

    # Since there is no grouping here, the aggregation is over all rows
    # and there will only be one output row from the query, which we can
    # print as follows:
    print('Tracks from {}: {}'.format(year[0], cursor.fetchone()[0]))
    # The [0] bits here tell us to pull the first column out of the 'year' tuple
    # and query results, respectively.

    # ADD YOUR CODE STARTING HERE
    # 1.
    print('-'*30)
    cursor.execute('SELECT a.artist_id, a.artist_name, at.term FROM artists a LEFT JOIN artist_term at ON a.artist_id = at.artist_id LEFT JOIN tracks t ON t.artist_id = a.artist_id WHERE t.title = "Silent Night (Album)"')
    print('Q1: The id, name, and term of the artist are',cursor.fetchall())
    print('-'*30)
    # 2.
    cursor.execute('SELECT distinct(title) FROM tracks WHERE duration < 3')
    print('Q2: unique track titles with the duration less than 3s are',cursor.fetchall())
    print('-'*30)
    # 3.
    cursor.execute('SELECT track_id FROM tracks WHERE year > 2008 AND year < 2012 ORDER BY duration DESC LIMIT 10')
    print('Q3: The ten track ids with longest duration are',cursor.fetchall())
    print('-'*30)
    # 4.
    cursor.execute('SELECT term FROM (SELECT term, COUNT(*) AS freq FROM artist_term GROUP BY term ORDER BY freq, term) AS t LIMIT 15')
    print('Q4: The 15 least frequently used terms are',cursor.fetchall())
    print('-'*30)
    # 5.
    cursor.execute('SELECT a. artist_name FROM artists a LEFT JOIN tracks t ON a.artist_id = t.artist_id ORDER BY t.duration LIMIT 1')
    print('Q5: The artist name associated with the shortest track duration is', cursor.fetchone()[0])
    print('-'*30)

    # 6.
    cursor.execute('SELECT AVG(duration) FROM tracks')
    print('Q6: the mean duration of all tracks is', cursor.fetchone()[0])
    print('-'*30)

    # 7.
    cursor.execute('SELECT track_id FROM tracks t LEFT JOIN (SELECT artist_id FROM artist_term GROUP BY artist_id Having COUNT(term) > 8) t2 ON t2.artist_id = t.artist_id ORDER BY track_id LIMIT 10')
    print('Q7: The first 10 track ids whose artists have more than 8 linked terms are', cursor.fetchall())
    print('-'*30)

    # 8.
    runtime_list = []
    for i in range(100):
        start_time = time.time()
        cursor.execute('SELECT a.artist_id, a.artist_name, at.term FROM artists a LEFT JOIN artist_term at ON a.artist_id = at.artist_id LEFT JOIN tracks t ON t.artist_id = a.artist_id WHERE t.title = "Silent Night (Album)"')
        end_time = time.time()
        runtime_list.append(end_time - start_time)

    cursor.execute('CREATE INDEX artid_ind ON tracks(artist_id)')
    cursor.execute('CREATE INDEX artid_ind2 ON artists(artist_id)')
    cursor.execute('CREATE INDEX artid_ind3 ON artist_term(artist_id)')

    runtime_list2 = []
    for i in range(100):
        start_time = time.time()
        cursor.execute('SELECT a.artist_id, a.artist_name, at.term FROM artists a LEFT JOIN artist_term at ON a.artist_id = at.artist_id LEFT JOIN tracks t ON t.artist_id = a.artist_id WHERE t.title = "Silent Night (Album)"')
        end_time = time.time()
        runtime_list2.append(end_time - start_time)

    print('Q8: Without Index:', min(runtime_list))
    print('With Index:', min(runtime_list2))
    print('-'*30)

    # 9.
    cursor.execute('BEGIN TRANSACTION')
    cursor.execute('DELETE FROM tracks WHERE track_id IN (SELECT t.track_id FROM tracks t LEFT JOIN artist_term at ON at.artist_id = t.artist_id WHERE at.term = "europop")')
    cursor.execute('SELECT count(*) FROM tracks')
    print('Q9: Number of records before rollback the delete:',cursor.fetchall())
    cursor.execute('ROLLBACK TRANSACTION')
    cursor.execute('SELECT count(*) FROM tracks')
    print('Q9: Number of records after rollback the delete:',cursor.fetchall())
