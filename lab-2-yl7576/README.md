# DS-GA 1004 Big Data
## Lab 2: Hadoop
- Prof Brian McFee (bm106)
- Dhara Mungra (dam797)
- Guido Petri (gp1655)

*Handout date*: 2021-02-25

*Submission deadline*: 2021-03-12, 23:55 EST

## 0. Requirements

Sections 1 and 2 of this assignment are designed to get you familiar with HPC and the workflow of running Hadoop jobs.

For full credit on this lab assignment, you will need to provide working solutions for sections 5 (matrix multiplication) and 6 (table joins).
You are provided with small example inputs for testing these programs, but we will run your programs on larger data for grading purposes.
Be sure to commit your final working implementations to git and push your changes before the submission deadline!

## 1. High-performance Computing (HPC) at NYU

This lab assignment will require the use of the
Hadoop cluster run by the NYU high-performance
computing (HPC) center.  To learn more about HPC at
NYU, please refer to the [HPC Wiki](https://sites.google.com/a/nyu.edu/nyu-hpc/).

By now, you should have received notification at
your NYU email address that your HPC account is active. If you have not received this notification yet, please contact the instructors immediately.

If you're new to HPC, please read through the
[tutorials](https://sites.google.com/a/nyu.edu/nyu-hpc/services/Training-and-Workshops/tutorials) section of the wiki, and - for this assignment in particular - the [MapReduce tutorial](https://sites.google.com/a/nyu.edu/nyu-hpc/services/Training-and-Workshops/tutorials/big-data-tutorial-map-reduce) section.

Logging into Peel on Linux or Mac from the NYU network is simple:
```bash
ssh netid@peel.hpc.nyu.edu
```
Uploading a file to Peel via SCP:
```bash
scp local_dir netid@peel.hpc.nyu.edu:peel_dir
```
Downloading a file from Peel via SCP:
```bash
scp netid@peel.hpc.nyu.edu:peel_dir local_dir
```

While it is possible to transfer files directly to and from Peel via SCP, we strongly recommend that you use git (and GitHub) to synchronize your code instead. This way, you can be sure that your submitted project is always up to date with the code being run on the HPC. To do this, you may need to set up a new SSH key (on Peel) and add it to your GitHub account; instructions for this can be found [here](https://docs.github.com/en/github/authenticating-to-github/adding-a-new-ssh-key-to-your-github-account).

**Note**: Logging into the HPC from outside the NYU
network can be somewhat complicated.  Instructions
are given
[here](https://sites.google.com/a/nyu.edu/nyu-hpc/documentation/hpc-access).

## 2. Hadoop and `mrjob`

In lecture, we discussed the Map-Reduce paradigm in the abstract, and did not dive into the details of the Hadoop implementation.  Hadoop is an open-source implementation of map-reduce written in Java.
In this lab, you will be implementing map-reduce jobs using `mrjob`, a Hadoop wrapper library in Python.

### Environment setup

To setup the required environment, you need to execute the following command in the Git repo when you log into Peel:
```bash
source shell_setup.sh
```

These modifications add shortcuts for interacting with the Hadoop distributed filesystem (`hfs`) and launching map-reduce jobs (`hjs`), as well as set up useful environment variables for `mrjob`.

*Note*: For convenience, you can copy-paste the contents of that file (or the `source` command itself, pointing to the correct path) into your `.bashrc` so that you don't need to re-run setup everytime you log in.

### Git on Peel

Follow [these instructions](https://help.github.com/en/github/authenticating-to-github/connecting-to-github-with-ssh) to clone your Github repo through SSH. Cloning via HTTPS may cause problems with HPC and will be deprecated soon. The repository includes three problems on MapReduce:

## 3. A first map-reduce project (Not for grading)

Included within the repository under `word_count/` is a full implementation of the "word-counting" program, and an example input text file (`book.txt`).

The program consists of two files:
```
src/mr_wordcount.py
src/run_mrjob.sh
```

The shell script loads the required module, `python/gcc/3.7.9`, and executes the MapReduceJob, which takes in a few command line arguments:

- `../book.txt` is the input used.
- `-r hadoop` indicates to `mrjob` that we are using a Hadoop backend. We can also simulate a Hadoop environment locally by removing this argument.
- `--hadoop-streaming-jar` tells `mrjob` where the Hadoop streaming file is located so that it can call the mappers and reducers appropriately.
- `--output-dir` indicates that the output of the MapReduce job should be placed in a specific folder (which you name) in HDFS. Keep in mind that duplicate filenames are not allowed on HDFS, so if you're using an existing folder name you'll have to remove the existing folder first.
- `--python-bin` and its value tell `mrjob` where the Python binary is so that the right version of Python can be used. This is the version loaded by the `module load` command.

The latest job result is then copied to the local file system and erased from the HDFS.

`mrjob` coordinates the communication between mappers and reducers for us in a very convenient fashion.

### Testing your mapper and reducer implementations using CPUs

Before we move on, it's a good idea to run these programs locally so we know what to expect.  (*Hint*: this is also an easy way to debug, as long as you have a small input on hand!) Thankfully, `mrjob` makes our life easy: all we have to do is run the Python file containing our MapReduce job definition and the corresponding job run call, and it'll simulate Hadoop locally:

```bash
python mr_wordcount.py ../book.txt
```

After running this command, you should see the total counts of each word in `book.txt`!
Remember, we did this all on one machine without using Hadoop, but you should now have a sense of what a MapReduce job looks like.

### Launching word-count on Hadoop cluster

Usually, to use Hadoop, we need to place input files in the Hadoop Distributed File System (HDFS), which is a completely different file system than the Unix file system. This is done via the following command:

```bash
hfs -put book.txt
```

To then launch the MapReduce job **through Hadoop streaming**, we have to use `hjs` with some command line arguments indicating the mapper, reducer, inputs and outputs:

```bash
hjs -file src/ -mapper mapper.sh -reducer reducer.sh -input book.txt -output word_count.out
```

This looks complicated! `mrjob` to the rescue! This is all taken care of internally by `mrjob`, which copies the input files to HDFS and runs your predefined mapper and reducer tasks.

After the job finishes, the result is stored in HDFS:

```bash
hfs -ls word_count
```

You should see a few nested directories showing your job's results in several file "parts", each corresponding to a single reducer node.

To retrieve the results of the computation, run

```bash
hfs -get word_count
```

to get all the partial outputs, or if you want the entire output as one file, run

```bash
hfs -getmerge word_count word_count_total.out
```
After running these commands, the results of the computation will be available to you through the usual Unix file system.

You can then safely remove the results from HDFS:
```bash
hfs -rm -r word_count
```

At this point, you should now have successfully run your first Hadoop job!

## 4. Select, filter and aggregate

For your first MapReduce program, you will translate an SQL query into a distributed computing job. In the `filter/` directory, you'll find some skeleton code in the `src` folder and some input data for your job. The `movies.csv` file has one movie-genre combination per line with the format

```
movie name, movie genre
```

where, if a movie belongs to several genres, there will be one line for each genre. Your task is to count the number of movies in each genre, ignoring any lines that have the genre `(no genres listed)`. The SQL equivalent would be the following:

```sql
SELECT genre, count(distinct name)
FROM movies
WHERE genre != '(no genres listed)'
GROUP BY genre
```

## 5. Matrix products

In the next part of this assignment, you will develop a map-reduce algorithm for matrix multiplication.  Recall that the product of two matrices `A * B` where `A` has shape `(n, k)` and `B` has shape `(k, m)` is a matrix `C` of shape `(n, m)` where for each `i` and `j` coordinates,
```
C[i, j] = A[i, 1] * B[1, j] + A[i, 2] * B[2, j] + ... + A[i, k] * B[k, j]
```

In the `matmul/` directory, you will find skeleton code for implementing matrix multiplication `matmul/src/`.
Accompanying this is an example pair of matrices to multiply: `matmul/small/A.txt` and `matmul/small/B.txt`.
In this assignment, you can assume that `A` and `B` will always be of compatible dimension, and that `A` will be on the left side of the multiplication.

Your job in this part of the assignment is to fill in the skeleton code (`mr_matmul.py`) by designing the intermediate key-value format and implementing the rest of the program accordingly.

Note that the data now comes spread into two files (`A.txt` and `B.txt`), which must both be processed by the mapper. You shall have the output like `output.txt` as the product of matrix `A` and `B`. 

**Note**: the input and output files should be in the sparse format of the matrix (i.e. only nonzero entries of matrix `M` are recorded as tuples `i,j,M[i, j]`)

e.g. Any example of input and output are following: 
```
A:  0,0,1.0  B:  0,1,2.0    A*B:   0,0,6.0
    0,1,2.0      1,0,3.0           0,1,2.0
    1,0,3.0                        1,1,6.0
```

In `mrjob`, we can use several files as input as seen in `matmul/src/run_mrjob.sh`. Each file can be spread across multiple mappers. Within the mapper, the identity of the file currently being processed can be found in the environment variable `mapreduce.map.input.file`, which we can access using `jobconf_from_env('mapreduce.map.input.file')` as noted in the code comments.  (Hint: this will be useful in part 6 below!)

To run the job, issue the following command:
```bash
./run_mrjob.sh
```
from within the `matmul/src` directory.

The output of the computation can be found in `result.out` or can be retrieved from HDFS using `hfs -getmerge` like in the example section above.

**Note**: To test this result locally, use the following command:

```bash
python mr_matmul.py ../small/A.txt ../small/B.txt
```

### Why the "2 3" parameters?

The mapper needs to know the shape of the matrix that you will eventually produce when multiplying `A * B`: specifically, the number of rows in `A` (2-by-4 for the small example) and the number of columns in `B` (4-by-3). You will need to specify these values as parameters to the mapper script as shown above.

## 6. Tabular data

In the final section of the lab, you are given two data files in comma-separated value (CSV) format.
These data files (`joins/music_small/artist_term.csv` and `joins/music_small/track.csv`) contain the same music data from the previous lab assignment on SQL and relational databases.  Specifically, the file `artist_term.csv` contains data of the form
```
ARTIST_ID,tag string
```
and `track.csv` contains data of the form
```
TRACK_ID,title string,album string,year,duration,ARTIST_ID
```

No skeleton code is provided for this part, but feel free to adapt any code from the previous sections that you've already completed.

### 6.1 Joining tables

For the first part, implement a map-reduce program which is equivalent to the following SQL query:
```sql
SELECT  track.artist_id, track.track_id, artist_term.tag
FROM    track INNER JOIN artist_term 
ON      track.artist_id = artist_term.artist_id
WHERE   track.year > 1990
```

The program should be executable in a way similar to the matrix multiplication example:
```bash
./run_mrjob.sh
```

### 6.2 Aggregation queries

For the last part, implement a map-reduce program which is equivalent to the following SQL query:
```sql
SELECT    artist_term.tag, min(track.year), avg(track.duration), count(artist_term.artist_id)
FROM      track LEFT JOIN artist_term
ON        track.artist_id = artist_term.artist_id
GROUP BY  artist_term.tag
```
That is, for each artist ID, compute the maximum year of release, average track duration and the total number of terms matching the artist.  **Note**: the number of terms for an artist could be zero!

The program should also be executable similar to other examples:
```bash
./run_mrjob.sh
```
