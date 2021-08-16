#!/bin/bash

module purge
module load python/gcc/3.7.9

python 6.1.py ../artist_term.csv ../track.csv \
       -r hadoop \
       --hadoop-streaming-jar $HADOOP_LIBPATH/$HADOOP_STREAMING \
       --output-dir 6.1join \
       --python-bin /share/apps/peel/python/3.7.9/gcc/bin/python \
