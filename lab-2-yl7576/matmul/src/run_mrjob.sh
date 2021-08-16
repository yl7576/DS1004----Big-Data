#!/bin/bash

module purge
module load python/gcc/3.7.9

python mr_matmul.py ../small/A.txt ../small/B.txt \
       --result-colsize 3 \
       --result-rowsize 2 \
       -r hadoop \
       --hadoop-streaming-jar $HADOOP_LIBPATH/$HADOOP_STREAMING \
       --output-dir matmul \
       --python-bin /share/apps/peel/python/3.7.9/gcc/bin/python \
