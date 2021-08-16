# Lab 3: Spark and Parquet Optimization Report

Name:
 
NetID: 

## Part 1: Spark

#### Question 1: 
How would you express the following computation using SQL instead of the object interface: `sailors.filter(sailors.rating > 6).select(sailors.sid, sailors.sname, sailors.age)`?

Code:
```SQL

SELECT sid, sname, age
FROM sailors
WHERE rating > 6

```


Output:
```

+---+-------+----+
|sid|  sname| age|
+---+-------+----+
| 22|dusting|45.0|
| 31| lubber|55.5|
| 32|   andy|25.5|
| 58|  rusty|35.0|
| 64|horatio|16.0|
| 71|  zorba|35.0|
| 74|horatio|25.5|
+---+-------+----+

```


#### Question 2: 
How would you express the following using the object interface instead of SQL: `spark.sql('SELECT sid, COUNT(bid) from reserves WHERE bid != 101 GROUP BY sid')`?

Code:
```python

q2 = reserves.filter(reserves.bid != 101)\
            .groupBy(reserves.sid)\
            .agg(f.count(reserves.bid))
q2.show()

```


Output:
```

+---+----------+
|sid|count(bid)|
+---+----------+
| 22|         3|
| 31|         3|
| 74|         1|
| 64|         1|
+---+----------+

```

#### Question 3: 
Using SQL and (multiple) inner joins, in a single query, how many distinct boats did each sailor reserve? 
The resulting DataFrame should include the sailor's id, name, and the count of distinct boats. 
(Hint: you may need to use `first(...)` aggregation function on some columns.) 
Provide both your query and the resulting DataFrame in your response to this question.

Code:
```SQL

q3 = sailors.join(reserves, ['sid'], 'inner')\
           .groupBy(sailors.sid, sailors.sname)\
           .agg(f.countDistinct(reserves.bid))
q3.show()

```


Output:
```

+---+-------+-------------------+
|sid|  sname|count(DISTINCT bid)|
+---+-------+-------------------+
| 64|horatio|                  2|
| 22|dusting|                  4|
| 31| lubber|                  3|
| 74|horatio|                  1|
+---+-------+-------------------+

```

#### Question 4: 
Repeating the analysis from Q6.2 in Lab2, implement a query using Spark transformations
which finds for each artist term, compute the minimum year of release,
average track duration, and the total number of artists for that term (by ID).
What are the results for the ten terms with the longest average track durations?  
Include both your query code and resulting DataFrame in your response.

Code:
```python

q4 = tracks.join(artist_term, ['artistID'], 'left')\
          .groupBy(artist_term.term)\
          .agg(f.min(tracks.year), f.avg(tracks.duration).alias('avg_duration'), f.count(tracks.artistID))\
          .orderBy(f.col('avg_duration').desc())
q4.show(10)

```


Output:
```

+--------------------+---------+------------------+---------------+
|                term|min(year)|      avg_duration|count(artistID)|
+--------------------+---------+------------------+---------------+
|              bhajan|        0|1147.2648391723633|              4|
|         svart metal|     2010| 1003.232177734375|              1|
|  experimental noise|        0|1001.5255482991537|              3|
|          mixed jazz|     1998|  950.281982421875|              1|
|   heavy psychedelic|     1996| 917.0281372070312|              1|
|  middle eastern pop|        0| 845.2958374023438|              2|
|           pakistani|        0| 810.4124545142764|             21|
|              nippon|        0| 805.9815979003906|              2|
|electronic music ...|        0| 768.0648345947266|              6|
|    early electronic|        0| 768.0648345947266|              6|
+--------------------+---------+------------------+---------------+

```
#### Question 5: 
Create a query using Spark transformations that finds the number of distinct tracks associated
(through artistID) to each term.  Modify this query to return only the top 10 most popular terms, and again for the bottom 10.
Include each query and the tables for the top 10 most popular terms and the 10 least popular terms in your response. 

##### 10 Most Popular Terms
Code:
```

q5_top = artist_term.join(tracks, ['artistID'], 'left')\
                        .groupBy(artist_term.term)\
                        .agg(f.countDistinct(tracks.trackID).alias('num_track'))\
                        .orderBy(f.col('num_track').desc())
q5_top.show(10)

```
Output:
```

+----------------+---------+
|            term|num_track|
+----------------+---------+
|            rock|    21796|
|      electronic|    17740|
|             pop|    17129|
|alternative rock|    11402|
|         hip hop|    10926|
|            jazz|    10714|
|   united states|    10345|
|        pop rock|     9236|
|     alternative|     9209|
|           indie|     8569|
+----------------+---------+

```

##### 10 Least Popular Terms
Code:
```

q5_bottom = artist_term.join(tracks, ['artistID'], 'left')\
                       .groupBy(artist_term.term)\
                       .agg(f.countDistinct(tracks.trackID).alias('num_track'))\
                       .orderBy('num_track')
q5_bottom.show(10)

```

Output:
```

+--------------------+---------+
|                term|num_track|
+--------------------+---------+
|               sense|        0|
|      trance melodic|        0|
|          funky jazz|        0|
|     milled pavement|        0|
|    stonersludgecore|        0|
|       fonal records|        0|
|       ambient metal|        0|
| finnish death metal|        0|
|progressive melod...|        0|
|            techcore|        0|
+--------------------+---------+

```

## Part 2: Parquet Optimization:

What to include in your report:
  - Tables of all numerical results (min, max, median) for each query/size/storage combination for part 2.3, 2.4 and 2.5.
  - How do the results in parts 2.3, 2.4, and 2.5 compare?
  - What did you try in part 2.5 to improve performance for each query?
  - What worked, and what didn't work?

Basic Markdown Guide: https://www.markdownguide.org/basic-syntax/

#### 2.3
##### csv_avg_income output
###### People_small:
```
Times to run csv_avg_income 25 times on hdfs:/user/bm106/pub/people_small.csv

[6.15894079208374, 2.2396631240844727, 1.1535706520080566, 1.0073566436767578, 0.9634718894958496, 0.8622510433197021, 0.9949064254760742, 0.865018367767334, 0.7726593017578125, 0.689913272857666, 0.7428436279296875, 0.6117780208587646, 0.6326596736907959, 0.8175094127655029, 0.6584341526031494, 0.6953003406524658, 0.7001125812530518, 0.7130448818206787, 0.7772951126098633, 0.7235064506530762, 0.6960456371307373, 0.7583847045898438, 0.7781505584716797, 0.772463321685791, 0.7854244709014893]

Minimum, Median, and Maximum Time taken to run csv_avg_income 25 times on hdfs:/user/bm106/pub/people_small.csv:
0.6117780208587646;0.7726593017578125;6.15894079208374
```
###### People_median:
```
Times to run csv_avg_income 25 times on hdfs:/user/bm106/pub/people_medium.csv

[6.0131847858428955, 1.4973225593566895, 1.9206056594848633, 2.2063841819763184, 1.2276606559753418, 2.4897518157958984, 1.4416248798370361, 1.1223959922790527, 1.2045624256134033, 1.127823829650879, 1.040414810180664, 1.1810753345489502, 1.404020071029663, 0.9752578735351562, 1.074376106262207, 1.2492361068725586, 2.971027135848999, 1.3869681358337402, 2.753973960876465, 1.1941578388214111, 1.166611671447754, 1.07425856590271, 1.7084100246429443, 1.1227409839630127, 2.717698335647583]

Minimum, Median, and Maximum Time taken to run csv_avg_income 25 times on hdfs:/user/bm106/pub/people_medium.csv:
0.9752578735351562;1.2492361068725586;6.0131847858428955
```

###### People_large:
```
Times to run csv_avg_income 25 times on hdfs:/user/bm106/pub/people_large.csv

[31.860382795333862, 26.700559377670288, 26.92779779434204, 27.25909447669983, 27.107097625732422, 26.263893604278564, 26.464281797409058, 26.08479595184326, 26.00886559486389, 25.917969226837158, 26.227774620056152, 26.86603283882141, 25.952692985534668, 25.900388717651367, 25.954068183898926, 25.99592900276184, 26.101192474365234, 26.302072525024414, 26.26609754562378, 27.163119792938232, 26.230612754821777, 25.982558727264404, 26.04809331893921, 26.07941246032715, 26.36044216156006]

Minimum, Median, and Maximum Time taken to run csv_avg_income 25 times on hdfs:/user/bm106/pub/people_large.csv:
25.900388717651367;26.230612754821777;31.860382795333862
```

##### csv_max_income output
###### People_small:
```
Times to run csv_max_income 25 times on hdfs:/user/bm106/pub/people_small.csv

[5.34448766708374, 1.984586477279663, 0.7444818019866943, 0.7182307243347168, 0.6548209190368652, 0.6658608913421631, 0.7528908252716064, 0.6894352436065674, 0.747011661529541, 0.657822847366333, 0.548349142074585, 0.5943527221679688, 0.5631301403045654, 0.5676581859588623, 0.5066454410552979, 0.5669169425964355, 0.5632021427154541, 0.7044460773468018, 0.5909373760223389, 0.6421184539794922, 0.5582783222198486, 0.638843297958374, 0.5914084911346436, 0.6587567329406738, 0.4857194423675537]

Minimum, Median, and Maximum Time taken to run csv_max_income 25 times on hdfs:/user/bm106/pub/people_small.csv:
0.4857194423675537;0.6421184539794922;5.34448766708374

```
###### People_medium:
```
Times to run csv_max_income 25 times on hdfs:/user/bm106/pub/people_medium.csv

[5.828845977783203, 1.6367027759552002, 1.4721989631652832, 1.4475767612457275, 1.3426513671875, 1.2062962055206299, 1.172455072402954, 1.2910966873168945, 1.189823865890503, 1.1205320358276367, 1.2626965045928955, 1.2450072765350342, 1.033989667892456, 1.0421710014343262, 1.2915432453155518, 0.9474022388458252, 0.9282741546630859, 1.0496902465820312, 1.221418857574463, 0.9580249786376953, 1.0879621505737305, 1.0284597873687744, 1.20900559425354, 1.1966748237609863, 1.17445707321167]

Minimum, Median, and Maximum Time taken to run csv_max_income 25 times on hdfs:/user/bm106/pub/people_medium.csv:
0.9282741546630859;1.1966748237609863;5.828845977783203
```

###### People_large:
```
Times to run csv_max_income 25 times on hdfs:/user/bm106/pub/people_large.csv

[31.68443465232849, 27.772936582565308, 27.731074571609497, 27.900426149368286, 28.06503987312317, 28.24939274787903, 29.165700674057007, 30.802563905715942, 28.080305814743042, 29.148870944976807, 27.733590364456177, 27.52611231803894, 27.94140076637268, 27.361063241958618, 27.3559353351593, 27.45709776878357, 27.759098768234253, 27.820256233215332, 27.40821886062622, 27.4472758769989, 27.2639422416687, 27.767818927764893, 27.707205772399902, 27.134707927703857, 27.978593587875366]

Minimum, Median, and Maximum Time taken to run csv_max_income 25 times on hdfs:/user/bm106/pub/people_large.csv:
27.134707927703857;27.767818927764893;31.68443465232849
```

##### csv_anna output
###### People_small:
```
Times to run csv_anna 25 times on hdfs:/user/bm106/pub/people_small.csv

[4.305178642272949, 0.3866896629333496, 0.21394848823547363, 0.25089240074157715, 1.3691473007202148, 0.16736984252929688, 0.2688267230987549, 0.1527233123779297, 0.15024924278259277, 0.13771820068359375, 0.15583324432373047, 0.12502765655517578, 0.2169814109802246, 0.14114809036254883, 0.14497685432434082, 0.16345787048339844, 0.1447606086730957, 0.11851763725280762, 0.127030611038208, 0.1272139549255371, 0.1278703212738037, 0.13214802742004395, 0.11319637298583984, 0.12505531311035156, 0.11833500862121582]

Minimum, Median, Maximum Time taken to run csv_anna 25 times on hdfs:/user/bm106/pub/people_small.csv:
0.11319637298583984,0.14497685432434082,4.305178642272949
```
###### People_medium:
```
Times to run csv_anna 25 times on hdfs:/user/bm106/pub/people_medium.csv

[3.836420774459839, 0.6375689506530762, 0.6067600250244141, 0.4685804843902588, 0.46260833740234375, 0.435960054397583, 0.4632232189178467, 0.47193288803100586, 0.5429220199584961, 0.5105061531066895, 0.4446423053741455, 0.5071620941162109, 0.418651819229126, 0.4916369915008545, 0.5223433971405029, 0.43252086639404297, 0.5173227787017822, 0.4199669361114502, 0.43575239181518555, 0.49147939682006836, 0.46934080123901367, 0.4391939640045166, 0.40015125274658203, 0.45157837867736816, 0.4204409122467041]

Minimum, Median, Maximum Time taken to run csv_anna 25 times on hdfs:/user/bm106/pub/people_medium.csv:
0.40015125274658203,0.4685804843902588,3.836420774459839
```

###### People_large:
```
Times to run csv_anna 25 times on hdfs:/user/bm106/pub/people_large.csv

[31.700814247131348, 27.640528678894043, 27.985074281692505, 28.333802938461304, 27.64664053916931, 27.57197642326355, 27.572710752487183, 27.570059537887573, 28.39229464530945, 28.194836139678955, 27.665645837783813, 27.45742917060852, 27.48288869857788, 27.731408834457397, 27.316414833068848, 27.373276710510254, 27.38336968421936, 27.497302532196045, 27.65526270866394, 27.157203197479248, 27.462799549102783, 27.359760284423828, 27.100313663482666, 26.84345555305481, 26.982882499694824]

Minimum, Median, Maximum Time taken to run csv_anna 25 times on hdfs:/user/bm106/pub/people_large.csv:
26.84345555305481,27.570059537887573,31.700814247131348
```

#### 2.4
##### pq_avg_income output
###### People_small:
```
Times to run pq_avg_income 25 times on hdfs:/user/yl7576/people_small.parquet

[4.855825901031494, 0.9763543605804443, 0.8550136089324951, 0.7413041591644287, 0.739790678024292, 0.7459900379180908, 1.4188015460968018, 0.6598861217498779, 0.6739461421966553, 0.674572229385376, 0.6486849784851074, 0.6666736602783203, 0.5956556797027588, 0.6292898654937744, 0.648486852645874, 0.641385555267334, 0.6086430549621582, 0.6039652824401855, 0.6376762390136719, 0.6066575050354004, 0.54164719581604, 0.5004992485046387, 0.6000633239746094, 0.5766754150390625, 0.5750973224639893]

Minimum, Median, and Maximum Time taken to run pq_avg_income 25 times on hdfs:/user/yl7576/people_small.parquet:
0.5004992485046387;0.648486852645874;4.855825901031494
```

###### People_medium:
```
Times to run pq_avg_income 25 times on hdfs:/user/yl7576/people_medium.parquet

[4.970145225524902, 1.2367711067199707, 1.032322883605957, 0.8455531597137451, 0.826934814453125, 0.757591724395752, 0.6523971557617188, 0.753563404083252, 0.7105441093444824, 0.7381000518798828, 0.6763341426849365, 0.7481913566589355, 0.6517078876495361, 0.73610520362854, 0.6823379993438721, 0.7298190593719482, 0.6339590549468994, 0.5811796188354492, 0.6729650497436523, 0.6175041198730469, 0.6492786407470703, 0.59413743019104, 0.5761945247650146, 0.6879086494445801, 0.5662086009979248]

Minimum, Median, and Maximum Time taken to run pq_avg_income 25 times on hdfs:/user/yl7576/people_medium.parquet:
0.5662086009979248;0.6879086494445801;4.970145225524902
```

###### People_large:
```
Times to run pq_avg_income 25 times on hdfs:/user/yl7576/people_large.parquet

[9.806986093521118, 6.2610695362091064, 6.397525310516357, 5.123959302902222, 6.0478515625, 4.86500883102417, 4.825936794281006, 4.850061893463135, 4.9064781665802, 5.07378888130188, 5.22784686088562, 5.250080585479736, 4.621037006378174, 4.612023115158081, 4.969207286834717, 4.555905818939209, 4.492569923400879, 4.5240983963012695, 4.5427329540252686, 4.274162530899048, 4.319746494293213, 3.899052381515503, 3.6263835430145264, 3.4012691974639893, 3.5470242500305176]

Minimum, Median, and Maximum Time taken to run pq_avg_income 25 times on hdfs:/user/yl7576/people_large.parquet:
3.4012691974639893;4.825936794281006;9.806986093521118
```

##### pq_max_income output
###### People_small:
```
Times to run pq_max_income 25 times on hdfs:/user/yl7576/people_small.parquet

[3.8356242179870605, 1.5406787395477295, 0.7525386810302734, 0.7222723960876465, 0.7152960300445557, 0.7425909042358398, 0.6257922649383545, 0.6526522636413574, 0.6487979888916016, 0.5750987529754639, 0.5609614849090576, 0.5821647644042969, 0.6041269302368164, 0.7274248600006104, 0.6364681720733643, 0.6073529720306396, 0.5401620864868164, 0.5703356266021729, 0.5504143238067627, 0.5782854557037354, 0.6145346164703369, 0.5266897678375244, 0.5450034141540527, 0.47125244140625, 0.49974846839904785]

Minimum, Median, and Maximum Time taken to run pq_max_income 25 times on hdfs:/user/yl7576/people_small.parquet:
0.47125244140625;0.6073529720306396;3.8356242179870605
```

###### People_medium:
```
Times to run pq_avg_income 25 times on hdfs:/user/yl7576/people_medium.parquet

[4.854589939117432, 1.035252332687378, 0.837627649307251, 0.8636026382446289, 0.8006162643432617, 0.6988904476165771, 0.7168946266174316, 0.7648365497589111, 0.6721940040588379, 0.7145459651947021, 0.6607484817504883, 0.6116735935211182, 0.6040675640106201, 0.6071751117706299, 0.6304285526275635, 0.6351540088653564, 0.6209731101989746, 0.7071774005889893, 0.5995368957519531, 0.5896096229553223, 0.6165926456451416, 0.558281660079956, 0.5686330795288086, 0.513812780380249, 0.5406925678253174]

Minimum, Median, and Maximum Time taken to run pq_avg_income 25 times on hdfs:/user/yl7576/people_medium.parquet:
0.513812780380249;0.6351540088653564;4.854589939117432
```

###### People_large:
```
Times to run pq_max_income 25 times on hdfs:/user/yl7576/people_large.parquet

[7.456369876861572, 4.2575037479400635, 4.174701690673828, 4.100272178649902, 4.0829925537109375, 4.101243495941162, 3.8504228591918945, 4.082145929336548, 3.97269606590271, 3.8402035236358643, 3.9361889362335205, 3.887240171432495, 3.8104209899902344, 4.032810926437378, 3.895132541656494, 3.7121386528015137, 3.715269088745117, 3.7059435844421387, 3.941861152648926, 3.764613628387451, 3.959993600845337, 3.660426616668701, 3.86625337600708, 3.5951662063598633, 3.6715848445892334]

Minimum, Median, and Maximum Time taken to run pq_max_income 25 times on hdfs:/user/yl7576/people_large.parquet:
3.5951662063598633;3.895132541656494;7.456369876861572
```


##### pq_anna output
###### People_small:
```
Times to run pq_anna 25 times on hdfs:/user/yl7576/people_small.parquet

[1.8936922550201416, 2.1909003257751465, 0.2614555358886719, 0.16110968589782715, 0.16179871559143066, 0.15434598922729492, 0.2983107566833496, 0.1408689022064209, 0.12067842483520508, 0.14286565780639648, 0.13505315780639648, 0.2382194995880127, 0.11967277526855469, 0.11855220794677734, 0.1084592342376709, 0.11248970031738281, 0.10927963256835938, 0.10957217216491699, 0.1365513801574707, 0.10655045509338379, 0.10337424278259277, 0.10577797889709473, 0.13379287719726562, 0.10073709487915039, 0.10456323623657227]

Minimum, Median, and Maximum Time taken to run pq_anna 25 times on hdfs:/user/yl7576/people_small.parquet:
0.10073709487915039;0.13379287719726562;2.1909003257751465
```

###### People_medium:
```
Times to run pq_anna 25 times on hdfs:/user/yl7576/people_medium.parquet

[3.3191511631011963, 0.33198070526123047, 0.2452237606048584, 0.19251465797424316, 0.34144091606140137, 0.16793346405029297, 0.16414737701416016, 0.1543135643005371, 0.15940427780151367, 0.14855146408081055, 0.14973855018615723, 0.1755237579345703, 0.13443231582641602, 0.1636674404144287, 0.13439512252807617, 0.1337435245513916, 0.13699913024902344, 0.13201904296875, 0.18254947662353516, 0.12845563888549805, 0.13395285606384277, 0.18730783462524414, 0.17694568634033203, 0.1488041877746582, 0.13603568077087402]

Minimum, Median, and Maximum Time taken to run pq_anna 25 times on hdfs:/user/yl7576/people_medium.parquet:
0.12845563888549805;0.15940427780151367;3.3191511631011963
```

###### People_large:
```
Times to run pq_anna 25 times on hdfs:/user/yl7576/people_large.parquet

[7.934858560562134, 6.976804494857788, 6.254330158233643, 5.628208875656128, 5.827971935272217, 5.901710748672485, 6.0453102588653564, 5.808146715164185, 5.6825032234191895, 6.000188827514648, 5.666940212249756, 5.933228254318237, 5.8051745891571045, 5.8953330516815186, 5.7771172523498535, 5.93207049369812, 5.850377321243286, 13.675693035125732, 5.2207136154174805, 5.7323384284973145, 5.924946069717407, 5.774433851242065, 5.848867893218994, 5.913157224655151, 5.758118629455566]

Minimum, Median, and Maximum Time taken to run pq_anna 25 times on hdfs:/user/yl7576/people_large.parquet:
5.2207136154174805;5.850377321243286;13.675693035125732
```

#### 2.5
In this part, I will try different optimization methods on people_large because the effect of optimization on large dataset will be more obvious for us to see the improvement of runtime. Also, different queries may benefit from different types of optimization, so I will analyze the query first and pick the most suitable optimization method. Each part has explanation.
##### Optimization Method1: sort
Sorting the data allows us to skip tons of rows that have same value and are not the ones we want to select. Therefore, it kinds of group rows according to values and help our searching become quicker. 
###### Test on pq_anna.py
For pq_anna.py, the query is mainly based on filter (filter by income and last name). Therefore, sorting the DataFrame according to these two columns and then writing out to parquet should work the best for this case. The output of pq_anna.py running on people_large dataset is shown below.Compare to runtime without optimization, the sort method makes an impressive improvement that the range of runtime is 2-5 seconds whereas the original runtime is around 5-13 seconds.  
```
Minimum, Median, and Maximum Time taken to run pq_anna 25 times on hdfs:/user/yl7576/people_large_sort.parquet:
2.162130832672119;2.3036515712738037;5.38198447227478
```
###### Test on pq_avg_income.py & pq_max_income.py
However, sorting method does not work for avg_income and max_income that have groupby queries. The original maximum runtimes for these two queries on large data are 7s and 9s, but after sorting, they become even slower with maximum runtimes equal to 12s and 10s. 
```
Minimum, Median, and Maximum Time taken to run pq_avg_income 25 times on hdfs:/user/yl7576/people_large_sort.parquet:
3.499197244644165;4.290985822677612;12.560113430023193

Minimum, Median, and Maximum Time taken to run pq_max_income 25 times on hdfs:/user/yl7576/people_large_sort.parquet:
5.685601472854614;6.115076541900635;10.932090759277344
```
##### Optimization Method2: Repartition
###### Repartition with columns for pq_anna.py
In this part, I tried to change the partition of the data. First, I repartitioned by zipcode and last name because those are columns that we apply group by. Then the data can be optimized for extracts by these columns. However, the maximum runtime increases a little bit in this case.
```
Minimum, Median, and Maximum Time taken to run pq_anna 25 times on hdfs:/user/yl7576/people_large_repart.parquet:3.819488048553467;4.058392524719238;18.580750703811646

Minimum, Median, and Maximum Time taken to run pq_avg_income 25 times on /user/yl7576/people_large_repart.parquet:6.66583251953125;11.706777334213257;359.16043043136597

Minimum, Median, and Maximum Time taken to run pq_max_income 25 times on /user/yl7576/people_large_repart.parquet:5.98010992932933;16.980266289842398;310.82953738498347
```
###### Repartition(2000) for pq_anna.py
In this part, I changed the number of partition and set it to 2000 because I tried to avoid low concurrency, data skewing and improper resource utilization caused by too few partitions. However, the maximum runtime is even longer. I think that is because too many partitions take more time for task scheduling than actual execution time.Also, generating the repartition operation is expensive.
```
Minimum, Median, and Maximum Time taken to run pq_anna 25 times on hdfs:/user/yl7576/people_large_repart2.parquet:
9.318537950515747;11.561248302459717;87.5191581249237

Minimum, Median, and Maximum Time taken to run pq_avg_income 25 times on hdfs:/user/yl7576/people_large_repart2.parquet:
11.04465651512146;11.637606620788574;17.504579544067383

Minimum, Median, and Maximum Time taken to run pq_max_income 25 times on hdfs:/user/yl7576/people_large_repart2.parquet:
8.823979385734999;10.692839520402149;19.923245434902324

```

##### Optimization Method3: replication factor
The replication factor is set to 3 by default. It means that even if 2 nodes that contain the chunk copies fail, the data can be accessed from its replicas from other nodes. However, if the chance of failure is negligible, replications is costly memory-wise. Also, replication factors defines number of communication overhead, so sometimes we want multiple threads if the query requires searching every row to make it faster. Here, I copied the people_large.parquet to a new directory in hdfs and set replication factor of the copied parquet equal to 1. The following test is based on the copied file. 

###### Test on pq_anna.py
In the query of pq_anna.py, the improvement is much bigger because the filter requires to search row by row. The maximum runtime improves from 13s to 5s. 

```
Minimum, Median, and Maximum Time taken to run pq_anna 25 times on /user/yl7576/copy_file/people_large.parquet:
1.965083122253418;2.193455696105957;5.258932113647461
```
###### Test on pq_avg_income.py & pq_max_income.py
For pq_avg_income, the maximum runtime improves from 8s to 7s, although the improvement is not dramatic, it can still show that reducing replication factor helps to optimize. However for pq_max_income, the maximum runtime even increases a little bit. So I don't think reducing relication factor work in this case. The reason for this might be that the query for avg_income and max_income does not really need to search every row.

```
Minimum, Median, and Maximum Time taken to run pq_avg_income 25 times on hdfs:/user/yl7576/copy_file/people_large.parquet:
3.8236188888549805;4.116208791732788;7.129131555557251

Minimum, Median, and Maximum Time taken to run pq_max_income 25 times on hdfs:/user/yl7576/copy_file/people_large.parquet:
3.7392057763832206;3.628648205527394;7.897264822548234
```

#### Part2 Summary
Comparing the result of 2.3, 2.4 and 2.5: Running queries on csv is the slowest. After transforming datasets to parquet, the runtime improves a lot. By using optimization in 2.5 based on parquets, sorting and replication factor work the best whereas the repartition does not really improve the performance. 



