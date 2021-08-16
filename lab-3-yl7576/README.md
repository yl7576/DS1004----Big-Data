# DSGA1004 - BIG DATA
## Lab 3: Spark and Column-Oriented Storage:
- Prof Brian McFee (bm106)
- Alec Hon (abh466)
- Anusha Patil(arp624)


*Handout date*: 2021-03-11

*Submission deadline*: 2021-03-26 23:55ET

### Contents:
 - [Part 1. Spark on Peel](#part-1-spark-on-peel)
     - [1.1 Getting started with pyspark](#11-getting-started-with-pyspark)
     - [1.2 Loading data](#12-loading-data)
     - [1.3 Creating views](#13-creating-views)
     - [1.4 What to turn in](#14-What-to-turn-in)
     - [1.5 SQL queries and DataFrame methods](#15-SQL-queries-and-DataFrame-methods) 
     - [1.6 Bigger datasets](#16-Bigger-datasets) 
 - [Part 2. Storage Optimization](#Part-2-Storage-Optimization)
     - [2.1 Getting started](#21-Getting-started)
     - [2.2 Helper functions](#22-Helper-functions)
     - [2.3 Benchmarking queries](#23-Benchmarking-queries) 
     - [2.4 CSV vs Parquet](#24-CSV-vs-Parquet) 
     - [2.5 Optimizing Parquet](#25-Optimizing-Parquet) 

---


## Part 1. Spark on Peel

In this assignment, we will be using Spark to analyze data on the Peel Cluster.

To run Spark jobs on the <b>Peel cluster</b>, first run the following command in the git repo:

```bash
source shell_setup.sh
```
The [shell script](./shell_setup.sh) prepares your current session for the lab by:
 - specifying system environment variables for Hadoop
 - loading the appropriate Python module on Peel's login node
 - setting the alias `hfs`, our usual alias for `hadoop fs`
 - setting the alias `spark-submit`, to streamline submitting pyspark jobs

Peel naturally has `Spark 2.4.0` loaded, which is the version we will be using for this lab.

For this assignment, you will be filling out the [lab_3_starter_code.py](./part_1_spark/lab_3_starter_code.py) in the `part_1_spark` folder and [Report.md](/Report.md) file found in the root of this repository.

### 1.1 Getting started with pyspark

For this section, we will be loading data and implementing basic SQL queries with Spark-SQL, 
and Spark transformations. Before beginning this assignment, please make sure that the data in 
this repository is on HDFS. The `artist_term.csv` and `tracks.csv` files are the same files as Lab 2,
but the `boats.txt`, `reserves.json`, and `sailors.json` are new data files. As an example and a reminder,
to load data onto HDFS, use the following command(assuming that you have already ran `shell_setup.sh`):

```bash
hfs -put boats.txt
```

Do the same for the `reserves.json` and `sailors.json` files, as well as the `artist_term.csv` and `tracks.csv` if you have not done so from Lab 2.

To utilize spark from our python script, we must first construct a Spark session. The following code block creates a Spark session, which we can pass into functions with other arguments that utilize Spark.

```python
if __name__ == '__main__':
    spark = SparkSession.builder.appName('Spark_Session_Name').getOrCreate()
    main(spark, other_arguments)
```

Before beginning the assignment, please run the `lab_3_starter_code.py` once to familiarize yourself with the output of the starter code. To submit a pyspark job to the cluster, call `spark-submit` on the python file in the correct directory containing the job:

```bash
spark-submit lab_3_starter_code.py 
```
Once you submit the job to Peel, Spark will continuously output a log until completion.  In this log, you will receive a tracking URL, 
which you can check if you are on the NYU wifi or VPN. An example is given below:

```bash
tracking URL: http://horton.hpc.nyu.edu:8088/proxy/application_1613664569968_2108/
```

The <b>Application ID</b> is the last part of url, or in the example above, `application_1613664569968_2108`. Note that your <b>Application ID</b> will be different every time you `spark-submit`a job. To see the output of the scripts,
run the following command:

```bash
yarn logs -applicationId <your_application_id> -log_files stdout
yarn logs -applicationId <your_application_id> -log_files stderr
```
With the example application ID from above, this would be as follows:

```bash
yarn logs -applicationId application_1613664569968_2108 -log_files stdout
```

The `stdout` log will show you all of your output, as well as standard errors if your code has any bugs. The `stderr` log may display additional information if your job fails.
(*Tip*: Most python errors will print to `stdout`, so check there first!)

### 1.2 Loading data 
The following code blocks are from the example query given in the starter code. To see the output of the example code, check the `stdout` from  `yarn logs` as described above after submitting the job.
We first load in the `boats.txt` data with the following line.

```python
boats = spark.read.csv(f'hdfs:/user/{netID}/boats.txt')
```
This first line loads the file `boats.txt` as a comma-separated values (CSV) file. To read json files, use the
`spark.read.json(file_path)` function. An example is shown below:

```python
sailors = spark.read.json(f'hdfs:/user/{netID}/sailors.json')
```

Once a DataFrame is created, you can print its contents by calling the `show()` method. This is not done in the starter code to save output space.
```python
boats.show()
sailors.show()
```
You can also print the dataframe's (inferred) schema by calling the `printSchema()` method:
```python
boats.printSchema()
sailors.printSchema()
```

In the output from running the script once, you'll notice that the data loaded from JSON file (`sailors`) has
type information and proper column headers, while the data loaded from CSV has
string type on all columns and no column headers.  *Why might this be?*

You can fix this by specifying the schema for `boats` on load:
```python
boats = spark.read.csv('boats.txt', schema='bid INT, bname STRING, color STRING')
boats.printSchema()
```
After providing the schema, you should now see that the `boats` DataFrame has a
correct type for each column, and proper column names.

### 1.3 Creating views

Once you have DataFrames loaded, you can register them as temporary views in your
spark session:
```python
boats.createOrReplaceTempView('boats')
sailors.createOrReplaceTempView('sailors')
```
Remember that a `view` in RDBMS terms is a relation that gets constructed at run-time.
Registering your DataFrames as views will make it possible to execute SQL queries just as you would in a standard RDBMS:
```python
results = spark.sql('SELECT count(*) FROM boats')
```
This creates a new DataFrame object `results`, but remember that all computation in
Spark is lazy: no data will be processed until you ask for it!  For example, you can
print the results:
```python
results.show()
```
or iterate over each row(not shown in the starter code):
```python
for row in query.collect():
    print(row)
```
### 1.4 What to turn in

For this assignment, you will complete the [lab_3_starter_code.py](./part_1_spark/lab_3_starter_code.py), adding your code for the following questions after the example query.
You will also edit and submit the [Report.md](/Report.md) file, answering the questions listed
out below, and copying and pasting results from your `stdout` where needed. Don't forget to commit changes to Report.md!
As a reminder, instructions on how to get `stdout` is given in section 1.1.


### 1.5 SQL queries and DataFrame methods

As you've seen above, Spark-SQL makes it possible to use SQL to process dataframes without having a proper RDBMS like SQLite or Postgres.
You can also manipulate DataFrames directly from Python.  For example, the following are equivalent:
```python
res1 = spark.sql('SELECT * FROM sailors WHERE rating > 7')
res2 = sailors.filter(sailors.rating > 7)
```
While any query is possible using either interface, some things will be more naturally expressed in SQL, and some things will be easier in Python.
Having some fluency with writing SQL will make it easier to know when to use each interface. Before starting this section, make sure you have loaded
the `reserves.json` file.


- **Question 1**: How would you express the following computation using SQL instead
  of the object interface?  `sailors.filter(sailors.rating > 6).select(sailors.sid, sailors.sname, sailors.age)`

- **Question 2**: How would you express the following using the object interface
  instead of SQL?  `spark.sql('SELECT sid, COUNT(bid) from reserves WHERE bid != 101 GROUP BY sid')`

- **Question 3**: Using SQL and (multiple) inner joins, in a single query, how many
  distinct boats did each sailor reserve?  The resulting DataFrame should include
  the sailor's id, name, and the count of distinct boats.  (*Hint*: you may need to use `first(...)` aggregation function on some columns.) 
  Provide both your query and the resulting DataFrame in your response to this question.

### 1.6 Bigger datasets

In this section, you will use Spark to analyze the same data as in the previous lab assignment (Hadoop).
When doing this part of the assignment, think back about how you implemented these computations as map-reduce programs.

In the project repository, you will again find CSV files `artist_term.csv` and
`tracks.csv` as given in the previous assignment.

As a first step, load these files as spark DataFrames with proper schema.
Specifically, the `artist_term` file should have columns.

- `artistID`
- `term`

and the `tracks` file should have columns

- `trackID`
- `title`
- `release`
- `year`
- `duration`
- `artistID`

*Note*: You may need to look at the first few lines of each file to determine the column types!


- **Question 4**: Repeating the analysis from Q6.2 in Lab2, implement a query using Spark transformations
  which finds for each artist term, compute the minimum year of release,
  average track duration, and the total number of artists for that term (by ID).
  What are the results for the ten terms with the longest average track durations? Include both your query code and resulting DataFrame in your response.

- **Question 5**: Create a query using Spark transformations that finds the number of distinct tracks associated
  (through artistID) to each term.  Modify this query to return only the top 10 most popular terms, and again for the bottom 10.
  Include each query and the tables for the top 10 most popular terms and the 10 least popular terms in your response. 

## Part 2. Storage Optimization

In this part of the assignment, you will be comparing the speed of Spark queries against
dataframes backed by either CSV or Parquet file stores, and optimizing parquets to speed up queries.

### 2.1 Getting started

As reference data, you'll be using a synthetic dataset including names, annual
income, and 5-digit zip codes.  Three versions of the data are provided:

  - `hdfs:/user/bm106/pub/people_small.csv`: 5000 records (166 KB)
  - `hdfs:/user/bm106/pub/people_medium.csv`: 500,000 records (16.2 MB)
  - `hdfs:/user/bm106/pub/people_large.csv`: 50,000,000 records (1.6 GB)

The schema for these files is as follows:

  - `schema='first_name STRING, last_name STRING, income FLOAT, zipcode INT'`

In each of the sections below, you will run a set of DataFrame queries against each
version of the data.  This will allow you to measure how the different analyses and
storage engines perform at different scales of data.

Before you begin, don't forget to run the `shell_setup.sh`!

*Tip*: you may want to work through each part completely using just the small and
medium data before starting on the large data.  This will make it easier for you to 
debug your analysis and get familiar with the data.

### 2.2 Helper functions

In the `part_2_storage` directory, you will find a folder named `queries` and `lab_3_storage_template_code.py`. In the
queries folder, you will find the following files:

 - [basic_query.py](./part_2_spark/queries/basic_query.py)
 - [csv_max_income.py](./part_2_spark/queries/csv_max_income.py)
 - [csv_avg_income.py](./part_2_spark/queries/csv_avg_income.py)
 - [csv_anna.py](./part_2_spark/queries/csv_anna.py)
 - [pq_max_income.py](./part_2_spark/queries/pq_max_income.py)
 - [pq_avg_income.py](./part_2_spark/queries/pq_avg_income.py)
 - [pq_anna.py](./part_2_spark/queries/pq_anna.py)
 - [bench.py](./part_2_spark/queries/bench.py)

The first seven files import from the `bench.py` file, which is used to conduct
timing benchmarks on a spark DataFrame query.  Rather than providing the DataFrame
directly to the benchmark function, you will need to write a function that constructs
a new DataFrame when called.

An example of this is given in `basic_query.py`.  This function takes in the `spark` session object,
as well as the path to a CSV file, and returns a DataFrame of the first five people sorted by `last_name, first_name`.  

```python
top5 = basic_query(spark, 'hdfs:/user/bm106/pub/people_small.csv')

top5.show()
```
The output in `stdout` would then be:
```
+----------+---------+-------+-------+                                          
|first_name|last_name| income|zipcode|
+----------+---------+-------+-------+
|   Annette|   Abbott|72870.0|  14763|
|    Hailey|   Abbott|72182.0|  75097|
|   Jocelyn|   Abbott|56574.0|   3062|
|     Sheri|   Abbott|64952.0|  77663|
|     Sonya|   Abbott|86156.0|  79072|
+----------+---------+-------+-------+
```

Rather than benchmarking `top5` directly, instead benchmark it as follows:
```python
times = bench.benchmark(spark, 5, basic_query,'hdfs:/user/bm106/pub/people_small.csv')
print(times)
```
In `stdout`:
```
[6.869371175765991, 0.21157383918762207, 0.2251441478729248, 0.1284043788909912, 0.12465882301330566]
```

The usage of the benchmark function above constructs the query `5` times using the `people_small.csv` file, and returns a list of the time (in seconds) for each trial.

To simplify testing, we will place each query into its own script. The `basic_query.py`, `csv_max_income.py`, `csv_avg_income.py`, `csv_anna.py`, `pq_max_income.py`, `pq_avg_income.py`, and `pq_anna.py`  take the file path for the dataset you want to perform the query on. To submit your script along with `bench.py` to Spark, you must specify `bench.py` as an argument after `--py-files`, and then the query file you wish to run and the file path of the dataset you want to query off of.
```bash
spark-submit --py-files bench.py basic_query.py <your_data_file_path>
```
The code contains example code for `bench.benchmark(spark, 5, basic_query, file_path)`. Be sure to specify which file to run on or else it will not run! The CSV file paths are located [above](#21-Getting-started). 

The `lab_3_storage_template_code.py` is skeleton code to connect to an active spark job. Feel free to use it as a template for saving the .csv files as .parquet files as well as optimizing the parquet files([2.4](#24-Csv-vs-Parquet) and [2.5](#25-Optimizing-Parquet)). 

Remember, to get any outputs, you must print to `stdout`, which is obtained in the same steps as described in [section 1.1](#11-getting-started-with-pyspark)

### 2.3 Benchmarking queries

Using the `basic_query.py` file as a template, you will be filling out the following scripts:

  - `csv_avg_income.py`: Contains `csv_avg_income` function that returns a DataFrame which computes the average `income` grouped by `zipcode`.

  - `csv_max_income.py`: Contains `csv_max_income` function that returns a DataFrame which computes the maximum `income` grouped by `last_name`.

  - `csv_anna.py`: Contains `csv_anna` function that returns a DataFrame which filters down to only include people with `first_name` of `'Anna'` and `income` at least `70000`.

All three of these files should work similarly to the `basic_query.py` example: parameters are the `spark` session object and the path in HDFS to the CSV file.

After filling out each function, benchmark their performance on each of the three data sets in the `main` function of each file. 
Each benchmark should contain 25 trials.  Record the **minimum, median, and maximum** time to complete each of the queries on each of the three data files.


### 2.4 CSV vs Parquet

For each of the three data files (small, medium, and large) convert the data to
Parquet format and store it in your own HDFS folder (e.g.,`hdfs:/user/YOUR_NETID/people_small.parquet`).
The easiest way to do this is to load the CSV file into a DataFrame in your python script, 
and then write it out using the [`DataFrame.write.parquet()`](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html) method. Remember to use the `lab_3_storage_template_code.py` if you are having difficulty connecting to a Spark Session.

Having translated the three data files from CSV to Parquet, fill out the following files that operate on Parquet-backed files rather than CSV(*Hint*: `spark.read.csv` does not read parquet files!):

  - `pq_avg_income.py`
  - `pq_max_income.py`
  - `pq_anna.py`

All three queries are the same as the previous section. Once again, once you fill out the query function, repeat the benchmarking experiment from part [2.3](#23-Benchmarking-queries) in the `main` function using these parquet-backed sources instead of the CSV sources.  Again, report the **min, median, and max** time for each query on each of the three files.  How do they compare to the results in part 1?


### 2.5 Optimizing Parquet

In the final part, your job is to try to make things go faster!  In this section, you are not allowed to change any of the *query* code that you wrote in the previous step, but you *are* allowed to change how the files are stored in HDFS utilizing Python scripts. Remember to use the `lab_3_storage_template_code.py` if you are having difficulty connecting to a Spark Session.

There are multiple ways of optimizing parquet structures. Some things you may want to try (but not limited to):

  - Sort the DataFrame according to particular columns before writing out to parquet
  - Change the `partition` structure of the data.  This can be done in two ways:
      - `dataframe.repartition(...)` (as described in lecture) and then writing the resulting dataframe back to a new file
      - Explicitly setting the partition columns when writing out to parquet.  **WARNING**: this can be very slow!
  - Change the HDFS replication factor
  - **(Non-trivial)** Adjust configurations of parquet module in Spark.
  
Each of the three queries may benefit from different types of optimization, so you may want to make multiple copies of each file. Try **at least three** different ways mentioned above and search for the best configurations for each way.

*Hint*: you may want to look through the `explain()` output on each of your queries when choosing optimizations.

## What to turn in

  - The code for all of your queries(`basic_query.py`, `csv_max_income.py`, `csv_avg_income.py`, `csv_anna.py`, `pq_max_income.py`, `pq_avg_income.py`, and `pq_anna.py` files) in the `queries` folder.
  - In the same [Report.md](/Report.md) as Part 1, please write a brief report with the following information:
    - Tables of all numerical results (min, max, median) for each query/size/storage combination for parts 2.3, 2.4 and 2.5.
    - How do the results in parts  2.3, 2.4 and 2.5 compare?
    - What did you try in part 2.5 to improve performance for each query?
    - What worked, and what didn't work?

You **will not be graded for any of your code** other than the code in the `queries` folder. You will also **not** be graded on how well you optimize the Parquet files, but rather on the quality of your report as well as having attempted to optimize them.


