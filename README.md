# iknn-HA
incomplete k nearest neighbor query in postgresql using HA algorithm

## Algorithm Discription
### HA algorithm:
  Please see Mr. Gao's paper: <i><b>IkNN-TFS-Yunjun Gao-20150115</b></i>
### Initialization:
  1. Set an extra table to record field-nBins-nObject relations. In other words, how many bin are there on each field names, and how many objects at present have complete value on this field.
  2. On each field, sort all objects that are complete on this field and distribute them into bins. The number of objects in each bins are roughly equal (the difference between two arbitruary bins is no more than 1, and the number of objects are non-ascendent).
  3. For each object inserted, delete or updated, the bins are updated to maintain proper distribution. The maintaining algorithm is best described in code, please see the three triggers in pgsql/HAinit.sql.

### Query
  1. Enumerate each field of the query object;
  2. For each field that is complete in the query object, binary search the bins to find which bin (whichBin in code) should the query object belong to.
  3. Enumerate each binary-searched bin, compute the distance between each object in the bin and the query object, then do the following things under two situations:
    1. If the candidate set is not full, insert the object into the candidate set;
    2. If the candidate set is full, compare the newly computed distance with the largest distance in the candidate set. If the newly computed distance is smaller than the largest distance in the candidate set, do a substitution.
  4. The candidate set is maintained by a max-heap.
  5. Return all the tuples remained in the candidate set.

## How to use?
### 0. Install postgresql-server-dev fist
~~~terminal
	sudo apt-get install postgreslq-server-dev-all
~~~
### 1. Clone and enter my repo (in terminal)
~~~terminal
    git clone git@github.com:Travelinglight/iknn-HA.git
    cd iknn-HA
~~~

### 2. Import LPinit.sql (in postgresql)

~~~sql
    \i pgsql/HAinit.sql
~~~

### 3. Initialize a target table to support iknn query
You may want to use the sample table "test". Import the table before you initialize it:

~~~sql
	CREATE DATABASE iknn;
	\i iknn.sql
~~~

Now initialize the "test" table

~~~sql
    select hainit('test', 2,4,3,4);
~~~

Command format:

* 'test' is the table name
* 2,4,3,4 are the number of bins on each field. The table 'test' has four columns, so there are four numbers following the table name.

The hainit function automatically does these things:

  1. create a tmp table with column dimension, nbin, nobj;
  2. add a column to the original table: ha_id, recording the unique id for ha algorithm;
  3. create table for each bin, representing buckets, with the name habin\_[table name]\_[field name]\_id. e.g. habin\_test\_a0\_1. Each bin has two columns: val and ha_id;
  4. build up b-tree index on habin\_[table name]\_[field name]\_id at column val, to auto-sort the tuples with field value;
  5. distribute objects into bins;
  7. set triggers to maintain the three columns and the extra tables on insert, update and delete.

### 4. Make and install iknnLP function (in terminal)
~~~terminal
	cd c
	make
	sudo make install
	cd ..
~~~

### 5. Import iknnLP function (in postgresql)
~~~sql
	\i c/iknnHA.sql
~~~

### 6. Performing iknn query with LP althrothm
~~~sql
	select a, b, c, d, distance from iknnHA('find 3 nearest neighbour of (a0,a2,a3)(31,33,34) from test') AS (a int, b int, c int, d int, distance float);
~~~
* a0,a2,a3 are columns in the table _test_.
* 31,33,34 are values of the columns respectively.
* The tuples returned are those considered nearest with the query object.

### 7. Here's the result
~~~sql
 a | b  | c  | d  | distance ---+----+----+----+----------   | 82 | 43 | 38 |      232   | 17 |    | 35 |        4   |  2 |    | 39 |      100(3 rows)
~~~

### 8. Inport LPwithdraw.sql

~~~
    \i HAwithdraw.sql
~~~

### 9. Withdraw iknn query support

~~~
    select hawithdraw([table name]);
~~~

This function automatically does these things:
  1. drop the extra column (ha_id) of the original table;
  2. drop all tmp tables;
  3. drop the three triggers that maintains the tmp table and the bins;

## Q&A
### I cannot create hstore extension when importing LPinit.sql.
  In ubuntu, you need to install the contrib before you create them.

  ~~~
  sudo apt-get install postgresql-contrib-9.4
  ~~~

  or you can install postgresql with those contribs

  ~~~
  sudo apt-get install postgresql postgresql-contrib
  ~~~

## Contact us
1. You can get the paper from Mr. Gao: gaoyj@zju.edu.cn
2. The projet is coded by Kingston Chen, feel free to ask any questions: holaelmundokingston@gmail.com