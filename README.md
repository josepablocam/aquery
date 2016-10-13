AQuery to Q (A2Q)
----------------

A2Q is a project that explores order-based optimizations for a simple query
language. Optimizations are mainly targeted as AST rewrites and, though simple,
yield empirical advantages at runtime.

It compiles [AQuery](http://dl.acm.org/citation.cfm?id=1315482) into [q](https://kx.com/),
providing users with two advantages:

* Allows users to query kdb+ databases with a syntax similar to SQL
* Heuristic, order-based optimizations, which provide performance advantages, while maintaining
readability in the source code
* Transparent compilation into q code (allowing experienced users to inspect the code executed)

The transformations on queries provide the same semantics at a fraction of the cost. These involve:
pushing selections below sorting, removing unnecessary sorts, maintaining order across local tables,
amongst others.


## Updates
This is a clean, Scala-based, rewrite of the AQuery system. The original implementation was developed
in C. Note that the C implementation is no longer being supported and we encourage users to switch to the
Scala version.


## Installation/Building
If you are interested in using AQuery, the steps below should provide you
with all the information needed to build on your system. Note that A2Q is meant
for \*nix. There are no plans on making this available for windows.

As AQuery currently compiles into q code, it is essential to have the kdb+
executable installed. You can visit [kx systems](https://kx.com/software-download.php)
to download the appropriate version for your machine. For licensing reasons, we
cannot include this for you. However, the process is simple.

Once you install q, make sure you add its installation location to your path.

```
export PATH=$PATH:/my/q/installation/location/
```

Calling `which q` should echo back the appropriate location, if
added succesfully.

Most uses of q in examples/demos will assume you have this appropriately setup.

Compilation of the AQuery translator is straightforward and handled exclusively by sbt (the Scala build tool used
in the project). Installing sbt is straightforward and there are plenty of resources online. Once you have sbt installed
it is a simple as 

The first step is to clone this git repository

```
git clone git@github.com:josepablocam/aquery.git
```

You can then build by running

```
cd aquery/
sbt assembly
```

This will save the AQuery jar as `target/scala-2.11/aquery.jar`.

To get basic usage information you can run

```
java -jar aquery.jar -h
```

For convenience, we recommend creating a simple bash script that wraps the call to java and adding this to your path.

```
cat ./a2q
#!/bin/bash
java -jar target/scala-2.11/aquery.jar $*
```

For following examples, we assume that we have this script available by the name ```a2q``` and you have added the location
of the script to your `PATH` variable (so that you can call as simply `a2q`).


The most common command will likely be
translating AQuery to q code, with optimizations. This can be achieved with

```
a2q -a 1 -s -c -o my_output_file.q my_input_file.a
```

or equally

```
a2q -a 1 -s -c my_input_file.a > my_output_file.q
```

(Note that the file endings are not significant).


## Basic Grammar
The grammar for AQuery is simple and should be familiar to
most people familiar with SQL. A good way to get a thorough overview
of the grammar is to explore the parser rules in `src/parser/aquery.y`.
These are written for Bison, and should be fairly readable. We provide a simple
summary here of the most frequently used constructs: queries, data
creation, loading/saving data, and user-defined functions.
We use `ID` to stand for identifier, and `epsilon` to stand
for the standard notation.

```
program : [full-query | create | insert | load | dump | udf | verbatim-q]*

// aquery files allow standard q code within special markers
verbatim-q: <q> q code </q>

/********* Queries *********/
full-query : [local_queries] query

local_queries: WITH local_query+

local_query: ID ['('columns')'] AS '(' query ')'

query : SELECT projections FROM source order-clause where-clause groupby-clause

projections: [val as ID | val] (, [val as ID | val])*

source : ID [ID | AS ID] |
  ID INNER JOIN source USING (columns) |
  ID FULL OUTER JOIN source USING (columns) |
  fun_call

// fun can be verbatim-q function constructing table
// or FLATTEN/DISTINCT/CONCATENATE
fun_call: fun'('val (,val)*')'

order-clause: ASSUMING ([ASC|DESC] ID)+

where-clause: WHERE search-conditions;

groupby-clause: GROUP BY val [as ID] (, val [as ID])* [HAVING search-conditions]

// where search condition is a boolean-yielding expression
search-conditions: search-condition (AND search-condition)*;

/********* Creating data *********/
create: CREATE TABLE ID [AS query | '(' schema ')']
schema: ID type (, ID type)*

insert: INSERT INTO ID [query | VALUES '(' vals ')']
vals: val (, val)*;

/********* Loading/Saving data *********/
load: LOAD DATA INFILE str INTO TABLE ID FIELDS TERMINATED BY str

save: query INTO OUTFILE str FIELDS TERMINATED BY str

/********* User defined functions *********/
udf: FUNCTION ID '(' arg-list ')' '{' fun-body '}'
arg_list: ID (, ID)*
fun_body: val | ID := val;

/********* Expressions *********/
val: val binop val | fun_call | -val | ID | int |
  float | datetime | str | date | hex
fun: ID | abs| avg[s] | count | deltas | distinct | drop
  | fill | first | last | max[s] | min[s] | mod | next
  | prev | sum[s] | stddev | ratios | vars | moving

binop: +|-|=|*|!=|<|>|>=|<=|&&|||
```

### Basic Examples
Following the commonly user grammar above, we provide some basic examples.

#### Queries
AQuery does not allow nested queries, but you can easily use `WITH` to construct
as many intermediate, query-local tables. These tables are only available
in subsequent queries within the same `WITH` statement.

```
WITH t1(c1, c2, c3) AS (
  SELECT c10, c20, c30 FROM t ASSUMING ASC c4
  WHERE f(c3) > 10 AND c5 != c6
  )
SELECT c1, sum(c2) FROM t1 GROUP BY c1
```
```
WITH nested_t1(c1, c2, c3) AS (
  SELECT c10, c20, c30 FROM t ASSUMING ASC c4
  WHERE f(c3) > 10 AND c5 != c6
  GROUP BY c10
  )
// must flatten out nested arrable using built-in FLATTEN
SELECT c1, sum(c2) FROM FLATTEN(nested_t1) GROUP BY c1
```

You are also free to have function calls that return a table as
part of the from-clause. For example

```
SELECT * FROM FLATTEN(CONCATENATE(nested_1, nested_2, nested_3))
```

which uses the built-ins `FLATTEN` and `CONCATENATE` (`DISTINCT` is also available
as a built-in). Users are also free to define functions (in verbatim-q for now)
that return tables and use these in their queries.

```
<q>f:{([]c1: 1 2 3 1; c2:10 20 30 40)}</q>
SELECT * FROM f() WHERE c1 = 1
```

The only restrictions on these functions currently is that you cannot use
dot notation to access table columns in the call. So for example

```
<q>
h:{([]c1:x * 2)};
t:([]c1:1 2 3);
</q>
SELECT * FROM h(t.c1)
```
is not valid. However, there is a simple workaround

```
SELECT * FROM h(t("c1"))
```


#### Creating Data
You can create data directly in AQuery as done below, but often
the use of verbatim q `<q> q commands here </q>` allows for simple
data creation for experiments.

```
CREATE TABLE my_table (c1 INT, c2 INT, c3 STRING)
INSERT INTO my_table VALUES(10, 20, "example")
INSERT INTO my_table SELECT * FROM my_table
```

You can also create tables using a query. For example:

```
CREATE TABLE my_table_derived
AS
  SELECT c1, c2 * 2 as twice_c2 FROM my_table
```

Note that the query can include any valid query-construct, such as a `WITH` statement.

#### Loading and Saving Data
For this example, we'll create a simple csv file from the shell
command line

```
josecambronero demo$ echo "ID,val" > my_table.csv;\
 for i in {1..10}; do echo "$i,$((i * 10))" >> my_table.csv; done
```
We must first declare the schema of our table, as this is required to parse
values to the appropriate type.

```
CREATE TABLE my_table (ID INT, val INT)
```
We can now parse in the csv file and insert the records into our predefined table.

```
LOAD DATA INFILE "my_table.csv"
INTO TABLE my_table
FIELDS TERMINATED BY ","
```

We can now perform a query and save the results into a new file. In this case,
we use a pipe-delimited file instead.

```
SELECT ID, val, val * 2 as derived_val FROM my_table
INTO OUTFILE "new_my_table.csv"
FIELDS TERMINATED BY "|"
```
```
josecambronero demo$ cat new_my_table.csv
ID|val|derived_val
1|10|20
2|20|40
3|30|60
4|40|80
5|50|100
6|60|120
7|70|140
8|80|160
9|90|180
10|100|200
```

### User-Defined Functions
Users can define their own aggregates using a simple syntax,
described briefly in the grammar section. The main idea is: you can
define local variables using `:=`, consecutive commands must be concatenated
with `;`, and the final result of the function corresponds to the last expression.
If there is a `;` on the last expression, the function produces no result.
This closely mirrors function definitions in q, with some minor changes.

In the example
below, we define our own covariance aggregate, for use in our queries.
You are free to use other aggregates in the definition. In this case we use
built-ins such as `avg`,
`sum`, and `sqrt`, which are self-explanatory.

```
FUNCTION myCov(x, y) {
  center_x := x - avg(x);
  center_y := y - avg(y);
  num := sum(center_x * center_y);
  denom := sqrt(sum(center_x * center_x)) * sqrt(sum(center_y * center_y));
  num / denom
  }
```

#### Function Semantics

Arrables, the main data structure in AQuery, are designed to address values column-wise, as vectors.
So for example, given a function and query as

```
FUNCTION my_avg(v) {sum(v) / count(v)}
SELECT my_avg(c1) FROM my_table
```

the call to `my_avg` passes `c1` as a vector to `my_avg`, in contrast to passing
each value of `c1` separately. This vector-oriented mentality is critical to
AQuery (and the underlying q/kdb+ systems), and results in more intuitive
expressions.

(As a side-note, there is already a built-in called
  `avg` that calculates the average, but we reproduce
  our own here as an exercise in UDF writing).

As highlighted before, arrables can be nested. Consider the simple
table shown below:

```
>SELECT * FROM my_table
>
c1 c2
-----
1  10
1  20
2  30
2  40
3  50
```

We can create a nested-arrable by using a `GROUP-BY`.

```
>CREATE TABLE nested_my_table AS SELECT c1, c2 FROM my_table GROUP BY c1
>
c1 c2    
---------
1  10 20i
2  30 40i
3  ,50i
```

Note that `c2` now consists of nested vectors.

Now if you want to apply an aggregate on each of the vectors constituting
the nested `c2`, you need to use a higher-order function that modifies
the application of the function. These are currently not exposed
directly in AQuery, as they constitute a more advanced function. However,
users can directly access them using verbatim-q. In this case, they can
use q's [adverbs](http://code.kx.com/wiki/Reference/each).

So our `my_avg` can be modified to

```
<q>my_avg_each:my_avg'</q>
```


We can now apply the aggregate on a query on `nested_my_table` and obtain
the expected results:

```
>SELECT c1, my_avg_each(c2) as avg_val FROM nested_my_table
>
c1 avg_val
----------
1  15     
2  35     
3  50
```

Note that if an aggregate is used in the *same* query that has the `GROUP BY`
clause, then AQuery automatically introduces the necessary modifiers in the
aggregate application. So the following is equally valid:

```
>SELECT c1, my_avg(c2) as avg_val
>FROM my_table GROUP BY c1
>
c1 avg_val
----------
1  15     
2  35     
3  50
```
It is also worth highlighting that one level of nesting can be removed
by applying `FLATTEN` (currently the only function application allowed
in a `FROM` clause).

```
>SELECT * FROM FLATTEN(nested_my_table)
>
c1 c2
-----
1  10
1  20
2  30
2  40
3  50
```

We consider another example, in which we have two levels of nesting. We create
a very simple table meant to reflect data collected in a timed experiment.

```
>CREATE TABLE t(indiv INT, grp STRING, val INT)
>INSERT INTO t VALUES(1, "A", 1)
>INSERT INTO t VALUES(1, "A", 2)
>INSERT INTO t VALUES(1, "A", 3)
>INSERT INTO t VALUES(1, "A", 4)
>INSERT INTO t VALUES(2, "A", 2)
>INSERT INTO t VALUES(2, "A", 2)
>INSERT INTO t VALUES(2, "A", 4)
>INSERT INTO t VALUES(2, "A", 8)
>INSERT INTO t VALUES(3, "B", 10)
>INSERT INTO t VALUES(3, "B", 20)
>INSERT INTO t VALUES(3, "B", 30)
>INSERT INTO t VALUES(3, "B", 40)
>INSERT INTO t VALUES(4, "B", 20)
>INSERT INTO t VALUES(4, "B", 20)
>INSERT INTO t VALUES(4, "B", 40)
>INSERT INTO t VALUES(4, "B", 80)
>

>SELECT * FROM t
>
indiv grp val
-------------
1     A   1  
1     A   2  
1     A   3  
1     A   4  
2     A   2  
2     A   2  
2     A   4  
2     A   8  
3     B   10
3     B   20
3     B   30
3     B   40
4     B   20
4     B   20
4     B   40
4     B   80
```

We create our first level of nesting by grouping by individual and group (`indiv` and `grp`, respectively).

```
>CREATE TABLE nested_1_t AS
>SELECT indiv, grp, val as timeseries
>FROM t GROUP BY indiv, grp
>
indiv grp timeseries
---------------------
1     A   1  2  3  4
2     A   2  2  4  8
3     B   10 20 30 40
4     B   20 20 40 80
```

This resembles our prior example. We could apply an aggregate on each row of `timeseries` by using a modifier as described before.

But consider another experiment, in which we *fuse*
nested arrays, showing that we can create say
a global time series that is the average of the
given ones in a group.

```
>SELECT grp, avg(timeseries) as avg_time_series
>FROM nested_1_t GROUP BY grp
>
grp avg_time_series
-------------------
A   1.5 2  3.5 6   
B   15  20 35  60  
```

Let's look at the nested arrable, before we apply our
aggregate.

```
>SELECT grp, timeseries FROM nested_1_t GROUP BY grp
>
grp timeseries             
---------------------------
A   1 2 3 4     2 2 4 8    
B   10 20 30 40 20 20 40 80
```

Note that there are now two levels of nesting. Each row
consists of two separate vectors. If there were `n` individuals per group,
these would then be n-vectors and so forth.

So once again, the query with `GROUP BY` works as expected because AQuery
added the necessary modifiers to the aggregate application behind the scenes,
but if you want to do this in two separate steps, you need to use our modified
aggregate.


```
>WITH
>temp AS (SELECT grp, timeseries FROM nested_1_t GROUP BY grp)
>SELECT grp, my_avg_each(timeseries) as avg_time_series FROM temp
>
grp avg_time_series
-------------------
A   1.5 2  3.5 6   
B   15  20 35  60  
```

#### Filling in missing values
We now consider a common case in timeseries analysis: filling in missing values. Consider the the table defined below:

```
<q>
 t1:([]indiv:1 1 1 1 1; ts:0 1 2 3 4; val:10 20 30 40 50);
 t2:([]indiv:2 2 2; ts:2 3 4; val:30 40 50);
 t:t1,t2;
 </q>
 ```
 Individual two is missing observations for timestamp
 (represented here as an integer for simplicity) zero and one.

 In order to calculate correlation between the values for both individuals, we extend the timeseries for
 individual two.

```
<q>corEach:cor';</q>
FUNCTION fill_backward(x) {reverse(fills(reverse(x)))}

WITH
  ts AS (SELECT ts FROM t)
  indiv AS (SELECT indiv FROM t)
  complete AS (SELECT * FROM distinct(ts), distinct(indiv))
  filled AS (SELECT   
    indiv, ts,
    fill_backward(val) as filled_val
    FROM
    complete FULL OUTER JOIN t USING (indiv, ts)
    ASSUMING ASC ts
    GROUP BY indiv)
   SELECT
   f1.indiv as indiv1, f2.indiv as indiv2,
   corEach(f1.filled_val, f2.filled_val) as filled_corr
   FROM filled f1, filled f2 WHERE f1.indiv != f2.indiv
```   

`ts` and `indiv` are single column tables representing all timestamps
and individuals, respectively. `complete` is a cartesian-product of the
distinct values in the two tables, generating a complete history for each
individual. `filled` uses a UDF `fill_backward` to fill any missing values
with the following non-missing value (hence the name). This is used in
`complete` by joining our original table with our extended history table and
applying the UDF by individual. Finally, our resulting query uses the
filled data to calculate the correlation between the two individuals' timeseries.

If you have any doubts/issues/feedback, please do not hesitate to contact
[Jose Cambronero](mailto:jcamsan@mit.edu)



