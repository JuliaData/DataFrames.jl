# Split-apply-combine paradigm

In the previous section, we had discussed that in *DataFrames.jl* we have five functions
that can be used to perform transformations of columns of a data frame and we also know how
to sort and index our data frame. In SQL the *GroupBy* operation known as to aggregate our 
data and collect some interesting stats. The same functions we can also use for *GroupedDataFrame* 
with the difference that the transformations are applied to groups and then combined. Here, 
an important distinction is that `combine` again allows transformations to produce any number 
of rows and they are combined in order of groups in the *GroupedDataFrame*. On the other hand 
`select`, `select`, `transform`, and `transform!` require transformations to produce the same 
number of rows for each group as the source group and produce a result that has the same row
order as the *parent* data frame of *GroupedDataFrame* passed. This rule has two important 
implications:

- it is not allowed to perform `select`, `select!`, `transform`, and `transform!` operations on 
  a *GroupedDataFrame* whose groups do not cover all rows of the *parent* data frame;
- `select`, `select!`, `transform`, and `transform!`, contrary to `combine`, ignore the order of 
  groups in the *GroupedDataFrame*.

The data frame that we are going to use is *german.csv* dataframe and in this section I will cover the
case of transformation for *GroupedDataFrame*. 

```jldoctest dataframe
julia> using DataFrames

julia> using CSV

julia> german_ref = CSV.read(joinpath(dirname(pathof(DataFrames)),
                                      "..", "docs", "src", "assets", "german.csv"),
                              DataFrame)
1000×10 DataFrame
  Row │ id     Age    Sex     Job    Housing  Saving accounts  Checking accoun ⋯
      │ Int64  Int64  String  Int64  String   String           String          ⋯
──────┼─────────────────────────────────────────────────────────────────────────
    1 │     0     67  male        2  own      NA               little          ⋯
    2 │     1     22  female      2  own      little           moderate
    3 │     2     49  male        1  own      little           NA
    4 │     3     45  male        2  free     little           little
    5 │     4     53  male        2  free     little           little          ⋯
    6 │     5     35  male        1  free     NA               NA
    7 │     6     53  male        2  own      quite rich       NA
    8 │     7     35  male        3  rent     little           moderate
  ⋮   │   ⋮      ⋮      ⋮       ⋮       ⋮            ⋮                ⋮        ⋱
  994 │   993     30  male        3  own      little           little          ⋯
  995 │   994     50  male        2  own      NA               NA
  996 │   995     31  female      1  own      little           NA
  997 │   996     40  male        3  own      little           little
  998 │   997     38  male        2  own      little           NA              ⋯
  999 │   998     23  male        2  free     little           little
 1000 │   999     27  male        2  own      moderate         moderate
                                                  4 columns and 985 rows omitted
```

In the previous section, I explained about minilanguage. Lets have a look again. As we discussed 
that the simplest way to specify a transformation is:

`source_column => transformation => target_column_name`

Here, the `source_column` is passed as an argument to `transformation` and stored in `target_column_name`
column.

Now, lets take one example using `GroupedDataFrame`. In this example we are going to figure out how many
rows(observations) we have with respect to gender (for example if a person is male then it will show how 
many males have 0 jobs).

Now, lets have a look that how does the following syntax work. So, in our syntax there are two function:
`combine` & `groupby`. `combine` function takes the output of `groupby` function and then applies `nrow`
function to each group whereas `groupby` takes the data frame and columns we want to group it by. And, 
`nrow` function counts the number of rows in our data frame.

```jldoctest dataframe
julia> combine(groupby(german_ref, [:Job, :Sex]), nrow)
8×3 DataFrame
 Row │ Job    Sex     nrow
     │ Int64  String  Int64
─────┼──────────────────────
   1 │     0  male       10
   2 │     0  female     12
   3 │     1  male      136
   4 │     1  female     64
   5 │     2  male      433
   6 │     2  female    197
   7 │     3  male      111
   8 │     3  female     37
```

In the following example, we used an *anonymous* function which is also known as *lambda* function. It is the
equivalent of the above. In this example, we splitted the dataset called `german_ref` by the columns `:Job` &
`:Sex` and then for each smaller dataset `df` applied the function `nrow` to them. Finally, `combine` the results
into a data frame. 

```jldoctest dataframe
julia> combine(groupby(german_ref, [:Job, :Sex]), df -> nrow(df))
8×3 DataFrame
 Row │ Job    Sex     x1
     │ Int64  String  Int64
─────┼──────────────────────
   1 │     0  male       10
   2 │     0  female     12
   3 │     1  male      136
   4 │     1  female     64
   5 │     2  male      433
   6 │     2  female    197
   7 │     3  male      111
   8 │     3  female     37
```

In the following example, we will calculate the average age per gender's:

```jldoctest dataframe
julia> using StatsBase

julia> combine(groupby(german_ref, [:Sex]), df -> mean(df.Age))
2×2 DataFrame
 Row │ Sex     x1
     │ String  Float64
─────┼─────────────────
   1 │ male    36.7783
   2 │ female  32.8032
```

We may have a doubt that in the above output what's the column `:x1`. So, here we can pass almost any
function to `combine` and then we can actually create a new data frame with new column name `:Age_avg`. 
Lets see an example:

```jldoctest dataframe
julia> combine(
         groupby(german_ref, [:Sex]),
           df -> DataFrame(Age_avg = mean(df.Age))
           )
2×2 DataFrame
 Row │ Sex     Age_avg
     │ String  Float64
─────┼─────────────────
   1 │ male    36.7783
   2 │ female  32.8032
```

As we know, `groupby()` is used to group values. In the following example the output with the values
are grouped:

```jldoctest dataframe
julia> gd = groupby(german_ref, :Age)
GroupedDataFrame with 53 groups based on key: Age
First Group (2 rows): Age = 19
 Row │ id     Age    Sex     Job    Housing  Saving accounts  Checking account ⋯
     │ Int64  Int64  String  Int64  String   String           String           ⋯
─────┼──────────────────────────────────────────────────────────────────────────
   1 │   391     19  female      1  rent     rich             moderate         ⋯
   2 │   633     19  female      2  rent     little           NA
                                                               3 columns omitted
⋮
Last Group (2 rows): Age = 75
 Row │ id     Age    Sex     Job    Housing  Saving accounts  Checking account ⋯
     │ Int64  Int64  String  Int64  String   String           String           ⋯
─────┼──────────────────────────────────────────────────────────────────────────
   1 │   330     75  male        3  free     little           little           ⋯
   2 │   536     75  female      3  own      NA               little
                                                               3 columns omitted

julia> groupby(german_ref, [])
GroupedDataFrame with 1 group based on key:
First Group (1000 rows):
  Row │ id     Age    Sex     Job    Housing  Saving accounts  Checking accoun ⋯
      │ Int64  Int64  String  Int64  String   String           String          ⋯
──────┼─────────────────────────────────────────────────────────────────────────
    1 │     0     67  male        2  own      NA               little          ⋯
    2 │     1     22  female      2  own      little           moderate
    3 │     2     49  male        1  own      little           NA
    4 │     3     45  male        2  free     little           little
    5 │     4     53  male        2  free     little           little          ⋯
    6 │     5     35  male        1  free     NA               NA
    7 │     6     53  male        2  own      quite rich       NA
    8 │     7     35  male        3  rent     little           moderate
  ⋮   │   ⋮      ⋮      ⋮       ⋮       ⋮            ⋮                ⋮        ⋱
  993 │   992     23  male        1  rent     NA               little          ⋯
  994 │   993     30  male        3  own      little           little
  995 │   994     50  male        2  own      NA               NA
  996 │   995     31  female      1  own      little           NA
  997 │   996     40  male        3  own      little           little          ⋯
  998 │   997     38  male        2  own      little           NA
  999 │   998     23  male        2  free     little           little
 1000 │   999     27  male        2  own      moderate         moderate
                                                  4 columns and 984 rows omitted

julia> gd[1]
2×10 SubDataFrame
 Row │ id     Age    Sex     Job    Housing  Saving accounts  Checking account ⋯
     │ Int64  Int64  String  Int64  String   String           String           ⋯
─────┼──────────────────────────────────────────────────────────────────────────
   1 │   391     19  female      1  rent     rich             moderate         ⋯
   2 │   633     19  female      2  rent     little           NA
                                                               3 columns omitted

julia> last(gd)
2×10 SubDataFrame
 Row │ id     Age    Sex     Job    Housing  Saving accounts  Checking account ⋯
     │ Int64  Int64  String  Int64  String   String           String           ⋯
─────┼──────────────────────────────────────────────────────────────────────────
   1 │   330     75  male        3  free     little           little           ⋯
   2 │   536     75  female      3  own      NA               little
                                                               3 columns omitted

julia> gd[(Age=19,)]
2×10 SubDataFrame
 Row │ id     Age    Sex     Job    Housing  Saving accounts  Checking account ⋯
     │ Int64  Int64  String  Int64  String   String           String           ⋯
─────┼──────────────────────────────────────────────────────────────────────────
   1 │   391     19  female      1  rent     rich             moderate         ⋯
   2 │   633     19  female      2  rent     little           NA
                                                               3 columns omitted

julia> parent(gd) # get the parent DataFrame
1000×10 DataFrame
  Row │ id     Age    Sex     Job    Housing  Saving accounts  Checking accoun ⋯
      │ Int64  Int64  String  Int64  String   String           String          ⋯
──────┼─────────────────────────────────────────────────────────────────────────
    1 │     0     67  male        2  own      NA               little          ⋯
    2 │     1     22  female      2  own      little           moderate
    3 │     2     49  male        1  own      little           NA
    4 │     3     45  male        2  free     little           little
    5 │     4     53  male        2  free     little           little          ⋯
    6 │     5     35  male        1  free     NA               NA
    7 │     6     53  male        2  own      quite rich       NA
    8 │     7     35  male        3  rent     little           moderate
  ⋮   │   ⋮      ⋮      ⋮       ⋮       ⋮            ⋮                ⋮        ⋱
  994 │   993     30  male        3  own      little           little          ⋯
  995 │   994     50  male        2  own      NA               NA
  996 │   995     31  female      1  own      little           NA
  997 │   996     40  male        3  own      little           little
  998 │   997     38  male        2  own      little           NA              ⋯
  999 │   998     23  male        2  free     little           little
 1000 │   999     27  male        2  own      moderate         moderate
                                                  4 columns and 985 rows omitted

julia> vcat(gd...) # back to the DataFrame, but in different order of rows than the original
1000×10 DataFrame
  Row │ id     Age    Sex     Job    Housing  Saving accounts  Checking accoun ⋯
      │ Int64  Int64  String  Int64  String   String           String          ⋯
──────┼─────────────────────────────────────────────────────────────────────────
    1 │   391     19  female      1  rent     rich             moderate        ⋯
    2 │   633     19  female      2  rent     little           NA
    3 │    93     20  male        2  rent     NA               rich
    4 │   155     20  female      2  rent     little           little
    5 │   167     20  female      2  own      rich             moderate        ⋯
    6 │   188     20  male        2  own      moderate         little
    7 │   296     20  female      2  rent     NA               NA
    8 │   410     20  female      2  own      little           moderate
  ⋮   │   ⋮      ⋮      ⋮       ⋮       ⋮            ⋮                ⋮        ⋱
  994 │   163     70  male        3  free     little           moderate        ⋯
  995 │   186     74  female      3  free     little           moderate
  996 │   430     74  male        1  own      little           NA
  997 │   606     74  male        3  own      little           NA
  998 │   756     74  male        0  own      little           rich            ⋯
  999 │   330     75  male        3  free     little           little
 1000 │   536     75  female      3  own      NA               little
                                                  4 columns and 985 rows omitted

julia> DataFrame(gd) # the same
1000×10 DataFrame
  Row │ id     Age    Sex     Job    Housing  Saving accounts  Checking accoun ⋯
      │ Int64  Int64  String  Int64  String   String           String          ⋯
──────┼─────────────────────────────────────────────────────────────────────────
    1 │   391     19  female      1  rent     rich             moderate        ⋯
    2 │   633     19  female      2  rent     little           NA
    3 │    93     20  male        2  rent     NA               rich
    4 │   155     20  female      2  rent     little           little
    5 │   167     20  female      2  own      rich             moderate        ⋯
    6 │   188     20  male        2  own      moderate         little
    7 │   296     20  female      2  rent     NA               NA
    8 │   410     20  female      2  own      little           moderate
  ⋮   │   ⋮      ⋮      ⋮       ⋮       ⋮            ⋮                ⋮        ⋱
  994 │   163     70  male        3  free     little           moderate        ⋯
  995 │   186     74  female      3  free     little           moderate
  996 │   430     74  male        1  own      little           NA
  997 │   606     74  male        3  own      little           NA
  998 │   756     74  male        0  own      little           rich            ⋯
  999 │   330     75  male        3  free     little           little
 1000 │   536     75  female      3  own      NA               little
                                                  4 columns and 985 rows omitted
```

If `keepkeys=true`, the resulting DataFrame contains all the grouping columns in addition to those generated. 
In this case if the returned value contains columns with the same names as the grouping columns, they are 
required to be equal. If `keepkeys=false` and some generated columns have the same name as grouping columns, 
they are kept and are not required to be equal to grouping columns. In the previous example our grouping column
was `:Age` so when `keepkeys=false` it dropped the `:Age` column after creating a data frame.

```jldoctest dataframe
julia> DataFrame(gd, keepkeys=false) # drop grouping columns when creating a data frame
1000×9 DataFrame
  Row │ id     Sex     Job    Housing  Saving accounts  Checking account  Cred ⋯
      │ Int64  String  Int64  String   String           String            Int6 ⋯
──────┼─────────────────────────────────────────────────────────────────────────
    1 │   391  female      1  rent     rich             moderate               ⋯
    2 │   633  female      2  rent     little           NA
    3 │    93  male        2  rent     NA               rich
    4 │   155  female      2  rent     little           little
    5 │   167  female      2  own      rich             moderate               ⋯
    6 │   188  male        2  own      moderate         little
    7 │   296  female      2  rent     NA               NA
    8 │   410  female      2  own      little           moderate
  ⋮   │   ⋮      ⋮       ⋮       ⋮            ⋮                ⋮               ⋱
  994 │   163  male        3  free     little           moderate               ⋯
  995 │   186  female      3  free     little           moderate
  996 │   430  male        1  own      little           NA
  997 │   606  male        3  own      little           NA
  998 │   756  male        0  own      little           rich                   ⋯
  999 │   330  male        3  free     little           little
 1000 │   536  female      3  own      NA               little
                                                  3 columns and 985 rows omitted

julia> groupcols(gd) # gives the vector of names of grouping variables
1-element Vector{Symbol}:
 :Age

julia> valuecols(gd) # gives the vector of names of non-grouping variables
9-element Vector{Symbol}:
 :id
 :Sex
 :Job
 :Housing
 Symbol("Saving accounts")
 Symbol("Checking account")
 Symbol("Credit amount")
 :Duration
 :Purpose

julia> groupindices(gd) # group indices in parent(gd)
1000-element Vector{Union{Missing, Int64}}:
 49
  4
 31
 27
 35
 17
 35
 17
 43
 10
  ⋮
 16
  5
 12
 32
 13
 22
 20
  5
  9
```

We can get the set of `keys` for each group of the *GroupedDataFrame* `gd` as a *GroupKeys* object. Each `key` 
is a *GroupKey*, which behaves like a *NamedTuple* holding the values of the grouping columns for a given group. 
Unlike the equivalent *Tuple* and *NamedTuple*, these keys can be used to index into `gd` efficiently. The 
ordering of the keys is identical to the ordering of the groups of `gd` under iteration and integer indexing.

```jldoctest dataframe
julia> kgd = keys(gd)
53-element DataFrames.GroupKeys{GroupedDataFrame{DataFrame}}:
 GroupKey: (Age = 19,)
 GroupKey: (Age = 20,)
 GroupKey: (Age = 21,)
 GroupKey: (Age = 22,)
 GroupKey: (Age = 23,)
 GroupKey: (Age = 24,)
 GroupKey: (Age = 25,)
 GroupKey: (Age = 26,)
 GroupKey: (Age = 27,)
 GroupKey: (Age = 28,)
 ⋮
 GroupKey: (Age = 63,)
 GroupKey: (Age = 64,)
 GroupKey: (Age = 65,)
 GroupKey: (Age = 66,)
 GroupKey: (Age = 67,)
 GroupKey: (Age = 68,)
 GroupKey: (Age = 70,)
 GroupKey: (Age = 74,)
 GroupKey: (Age = 75,)
```

We can index into a *GroupedDataFrame* like to a vector or to a dictionary. The second form accepts *GroupKey*,
*NamedTuple* or a *Tuple*.

```jldoctest dataframe
julia> gd
GroupedDataFrame with 53 groups based on key: Age
First Group (2 rows): Age = 19
 Row │ id     Age    Sex     Job    Housing  Saving accounts  Checking account ⋯
     │ Int64  Int64  String  Int64  String   String           String           ⋯
─────┼──────────────────────────────────────────────────────────────────────────
   1 │   391     19  female      1  rent     rich             moderate         ⋯
   2 │   633     19  female      2  rent     little           NA
                                                               3 columns omitted
⋮
Last Group (2 rows): Age = 75
 Row │ id     Age    Sex     Job    Housing  Saving accounts  Checking account ⋯
     │ Int64  Int64  String  Int64  String   String           String           ⋯
─────┼──────────────────────────────────────────────────────────────────────────
   1 │   330     75  male        3  free     little           little           ⋯
   2 │   536     75  female      3  own      NA               little
                                                               3 columns omitted
```

Keys can be used as indices to retrieve the corresponding group from their *GroupedDataFrame*:

```jldoctest dataframe
julia> k = keys(gd)[1]
GroupKey: (Age = 19,)

julia> gd[k]
2×10 SubDataFrame
 Row │ id     Age    Sex     Job    Housing  Saving accounts  Checking account ⋯
     │ Int64  Int64  String  Int64  String   String           String           ⋯
─────┼──────────────────────────────────────────────────────────────────────────
   1 │   391     19  female      1  rent     rich             moderate         ⋯
   2 │   633     19  female      2  rent     little           NA
                                                               3 columns omitted

julia> gd[keys(gd)[1]] == gd[1]
true

julia> k = keys(gd)[1]
GroupKey: (Age = 19,)

julia> ntk = NamedTuple(k)
(Age = 19,)

julia> tk = Tuple(k)
(19,)
```

During `groupby`, sorting can also be done:

```jldoctest dataframe
julia> groupby(german_ref, :Age; sort=true)
GroupedDataFrame with 53 groups based on key: Age
First Group (2 rows): Age = 19
 Row │ id     Age    Sex     Job    Housing  Saving accounts  Checking account ⋯
     │ Int64  Int64  String  Int64  String   String           String           ⋯
─────┼──────────────────────────────────────────────────────────────────────────
   1 │   391     19  female      1  rent     rich             moderate         ⋯
   2 │   633     19  female      2  rent     little           NA
                                                               3 columns omitted
⋮
Last Group (2 rows): Age = 75
 Row │ id     Age    Sex     Job    Housing  Saving accounts  Checking account ⋯
     │ Int64  Int64  String  Int64  String   String           String           ⋯
─────┼──────────────────────────────────────────────────────────────────────────
   1 │   330     75  male        3  free     little           little           ⋯
   2 │   536     75  female      3  own      NA               little
                                                               3 columns omitted

julia> combine(gd, :Job => sum, nrow)
53×3 DataFrame
 Row │ Age    Job_sum  nrow
     │ Int64  Int64    Int64
─────┼───────────────────────
   1 │    19        3      2
   2 │    20       28     14
   3 │    21       25     14
   4 │    22       50     27
   5 │    23       79     48
   6 │    24       76     44
   7 │    25       72     41
   8 │    26       94     50
  ⋮  │   ⋮       ⋮       ⋮
  47 │    65        6      5
  48 │    66        8      5
  49 │    67        7      3
  50 │    68        5      3
  51 │    70        3      1
  52 │    74        7      4
  53 │    75        6      2
              38 rows omitted 
```

If `ungroup=true` (the default) a data frame is returned. If `ungroup=false` a `GroupedDataFrame` grouped using
`keycols(gdf)` is returned.

```jldoctest dataframe
julia> combine(gd, :Job => sum, nrow, ungroup=false)
GroupedDataFrame with 53 groups based on key: Age
First Group (1 row): Age = 19
 Row │ Age    Job_sum  nrow
     │ Int64  Int64    Int64
─────┼───────────────────────
   1 │    19        3      2
⋮
Last Group (1 row): Age = 75
 Row │ Age    Job_sum  nrow
     │ Int64  Int64    Int64
─────┼───────────────────────
   1 │    75        6      2

julia> combine(sdf -> sum(sdf.Job), gd) 
53×2 DataFrame
 Row │ Age    x1
     │ Int64  Int64
─────┼──────────────
   1 │    19      3
   2 │    20     28
   3 │    21     25
   4 │    22     50
   5 │    23     79
   6 │    24     76
   7 │    25     72
   8 │    26     94
  ⋮  │   ⋮      ⋮
  47 │    65      6
  48 │    66      8
  49 │    67      7
  50 │    68      5
  51 │    70      3
  52 │    74      7
  53 │    75      6
     38 rows omitted                                                             

julia> combine(gd, :Job => (x -> sum(log, x)) => :sum_log_Job) # specifying a name for target column
53×2 DataFrame
 Row │ Age    sum_log_Job
     │ Int64  Float64
─────┼────────────────────
   1 │    19     0.693147
   2 │    20     9.41638
   3 │    21     7.62462
   4 │    22    15.9424
   5 │    23  -Inf
   6 │    24  -Inf
   7 │    25  -Inf
   8 │    26  -Inf
  ⋮  │   ⋮         ⋮
  47 │    65  -Inf
  48 │    66  -Inf
  49 │    67     2.48491
  50 │    68  -Inf
  51 │    70     1.09861
  52 │    74  -Inf
  53 │    75     2.19722
           38 rows omitted

julia> combine(gd, [:Job, :Age] .=> sum) # passing a vector of pairs
53×3 DataFrame
 Row │ Age    Job_sum  Age_sum
     │ Int64  Int64    Int64
─────┼─────────────────────────
   1 │    19        3       38
   2 │    20       28      280
   3 │    21       25      294
   4 │    22       50      594
   5 │    23       79     1104
   6 │    24       76     1056
   7 │    25       72     1025
   8 │    26       94     1300
  ⋮  │   ⋮       ⋮        ⋮
  47 │    65        6      325
  48 │    66        8      330
  49 │    67        7      201
  50 │    68        5      204
  51 │    70        3       70
  52 │    74        7      296
  53 │    75        6      150
                38 rows omitted

julia> combine(gd) do sdf # dropping group when DataFrame() is returned
          sdf.Age[1] != 1 ? sdf : DataFrame()
       end
1000×10 DataFrame
  Row │ Age    id     Sex     Job    Housing  Saving accounts  Checking accoun ⋯
      │ Int64  Int64  String  Int64  String   String           String          ⋯
──────┼─────────────────────────────────────────────────────────────────────────
    1 │    19    391  female      1  rent     rich             moderate        ⋯
    2 │    19    633  female      2  rent     little           NA
    3 │    20     93  male        2  rent     NA               rich
    4 │    20    155  female      2  rent     little           little
    5 │    20    167  female      2  own      rich             moderate        ⋯
    6 │    20    188  male        2  own      moderate         little
    7 │    20    296  female      2  rent     NA               NA
    8 │    20    410  female      2  own      little           moderate
  ⋮   │   ⋮      ⋮      ⋮       ⋮       ⋮            ⋮                ⋮        ⋱
  994 │    70    163  male        3  free     little           moderate        ⋯
  995 │    74    186  female      3  free     little           moderate
  996 │    74    430  male        1  own      little           NA
  997 │    74    606  male        3  own      little           NA
  998 │    74    756  male        0  own      little           rich            ⋯
  999 │    75    330  male        3  free     little           little
 1000 │    75    536  female      3  own      NA               little
                                                  4 columns and 985 rows omitted

julia> combine(gd, :Job => :Job1, :Age => :Age1,
               [:Job, :Age] => +, keepkeys=false) # auto-splatting, renaming and keepkeys
1000×3 DataFrame
  Row │ Job1   Age1   Job_Age_+
      │ Int64  Int64  Int64
──────┼─────────────────────────
    1 │     1     19         20
    2 │     2     19         21
    3 │     2     20         22
    4 │     2     20         22
    5 │     2     20         22
    6 │     2     20         22
    7 │     2     20         22
    8 │     2     20         22
  ⋮   │   ⋮      ⋮        ⋮
  994 │     3     70         73
  995 │     3     74         77
  996 │     1     74         75
  997 │     3     74         77
  998 │     0     74         74
  999 │     3     75         78
 1000 │     3     75         78
                985 rows omitted

julia> combine(gd, :Job, :Age => sum) # passing columns and broadcasting
1000×3 DataFrame
  Row │ Age    Job    Age_sum
      │ Int64  Int64  Int64
──────┼───────────────────────
    1 │    19      1       38
    2 │    19      2       38
    3 │    20      2      280
    4 │    20      2      280
    5 │    20      2      280
    6 │    20      2      280
    7 │    20      2      280
    8 │    20      2      280
  ⋮   │   ⋮      ⋮       ⋮
  994 │    70      3       70
  995 │    74      3      296
  996 │    74      1      296
  997 │    74      3      296
  998 │    74      0      296
  999 │    75      3      150
 1000 │    75      3      150
              985 rows omitted

julia> combine(gd, [:Job, :Age] .=> Ref)
53×3 DataFrame
 Row │ Age    Job_Ref                            Age_Ref                       ⋯
     │ Int64  SubArray…                          SubArray…                     ⋯
─────┼──────────────────────────────────────────────────────────────────────────
   1 │    19  [1, 2]                             [19, 19]                      ⋯
   2 │    20  [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2…  [20, 20, 20, 20, 20, 20, 20,
   3 │    21  [2, 2, 2, 1, 2, 2, 2, 1, 2, 2, 2…  [21, 21, 21, 21, 21, 21, 21,
   4 │    22  [2, 2, 2, 2, 2, 2, 2, 2, 1, 2  ……  [22, 22, 22, 22, 22, 22, 22,
   5 │    23  [0, 3, 1, 2, 2, 2, 2, 2, 0, 2  ……  [23, 23, 23, 23, 23, 23, 23,  ⋯
   6 │    24  [2, 2, 2, 1, 2, 2, 1, 2, 2, 1  ……  [24, 24, 24, 24, 24, 24, 24,
   7 │    25  [2, 2, 1, 2, 2, 2, 2, 2, 2, 1  ……  [25, 25, 25, 25, 25, 25, 25,
   8 │    26  [2, 2, 1, 2, 2, 2, 2, 2, 2, 1  ……  [26, 26, 26, 26, 26, 26, 26,
  ⋮  │   ⋮                    ⋮                                  ⋮             ⋱
  47 │    65  [2, 0, 2, 2, 0]                    [65, 65, 65, 65, 65]          ⋯
  48 │    66  [3, 1, 3, 1, 0]                    [66, 66, 66, 66, 66]
  49 │    67  [2, 3, 2]                          [67, 67, 67]
  50 │    68  [0, 2, 3]                          [68, 68, 68]
  51 │    70  [3]                                [70]                          ⋯
  52 │    74  [3, 1, 3, 0]                       [74, 74, 74, 74]
  53 │    75  [3, 3]                             [75, 75]
                                                    1 column and 38 rows omitted

julia> combine(gd, AsTable(:) => Ref)
53×2 DataFrame
 Row │ Age    id_Age_etc_Ref
     │ Int64  NamedTuple…
─────┼──────────────────────────────────────────
   1 │    19  (id = [391, 633], Age = [19, 19]…
   2 │    20  (id = [93, 155, 167, 188, 296, 4…
   3 │    21  (id = [134, 194, 203, 208, 363, …
   4 │    22  (id = [1, 12, 67, 79, 143, 228, …
   5 │    23  (id = [47, 58, 59, 111, 118, 144…
   6 │    24  (id = [11, 39, 43, 82, 101, 102,…
   7 │    25  (id = [10, 17, 35, 52, 63, 69, 1…
   8 │    26  (id = [24, 41, 55, 64, 104, 171,…
  ⋮  │   ⋮                    ⋮
  47 │    65  (id = [179, 438, 624, 807, 883],…
  48 │    66  (id = [75, 137, 213, 723, 774], …
  49 │    67  (id = [0, 554, 779], Age = [67, …
  50 │    68  (id = [187, 846, 917], Age = [68…
  51 │    70  (id = [163], Age = [70], Sex = […
  52 │    74  (id = [186, 430, 606, 756], Age …
  53 │    75  (id = [330, 536], Age = [75, 75]…
                                 38 rows omitted
```

In this case, I used `(x -> identity(x))` transformation although in this case it would be enough just 
to write `:Sex`; the reason is that I wanted to highlight that if we use an anonymous function in the 
transformation definition we have to wrap it in `(` and `)` as shown above; otherwise the expression 
is parsed by Julia in an unexpected way due to the `=>` operator precedence level.

```jldoctes dataframe
julia> using Statistics

julia> combine(groupby(german_ref, :Job),
                      :Sex => (x -> identity(x)) => :Sex,
                      :Age => mean => :Age)
1000×3 DataFrame
  Row │ Job    Sex     Age
      │ Int64  String  Float64
──────┼────────────────────────
    1 │     0  female  40.0909
    2 │     0  male    40.0909
    3 │     0  male    40.0909
    4 │     0  male    40.0909
    5 │     0  female  40.0909
    6 │     0  male    40.0909
    7 │     0  female  40.0909
    8 │     0  male    40.0909
  ⋮   │   ⋮      ⋮        ⋮
  994 │     3  female  39.027
  995 │     3  female  39.027
  996 │     3  male    39.027
  997 │     3  female  39.027
  998 │     3  male    39.027
  999 │     3  male    39.027
 1000 │     3  male    39.027
               985 rows omitted
```

However, in some cases it would be more natural not to expand the `:Sex` column into multiple rows. We 
can easily achieve it by protecting the value returned by the transformation using `Ref` (just as in 
broadcasting):

```jldoctest dataframe
julia> combine(groupby(german_ref, :Job),
                      :Sex => (x -> Ref(identity(x))) => :Sex,
                      :Age => mean => :Age)
4×3 DataFrame
 Row │ Job    Sex
     │ Int64  SubArray…
─────┼─────────────────────────────────────────────
   1 │     0  ["female", "male", "male", "male…
   2 │     1  ["male", "male", "male", "male",…
   3 │     2  ["male", "female", "male", "male…
   4 │     3  ["male", "male", "female", "fema…
                                   1 column omitted
```

In this case the vectors produced by our transformation are kept in a single row of the produced data frame. 
Note that in this case we also could have just written:

```jldoctest dataframe
julia> combine(groupby(german_ref, :Job), :Sex => Ref => :Sex, :Age => mean => :Age)
4×3 DataFrame
 Row │ Job    Sex
     │ Int64  SubArray…
─────┼─────────────────────────────────────────────
   1 │     0  ["female", "male", "male", "male…
   2 │     1  ["male", "male", "male", "male",…
   3 │     2  ["male", "female", "male", "male…
   4 │     3  ["male", "male", "female", "fema…
                                   1 column omitted
```

as we were applying `identity` transformation to our data, which can be skipped.

`nrow` does not require passing source column name (but allows specifying of target column name; the default 
name is `:nrow`) and produces number of rows in the passed data frame. Here is an example of both:

```jldoctest dataframe
julia> combine(groupby(german_ref, :Sex), nrow, nrow => :count)
2×3 DataFrame
 Row │ Sex     nrow   count
     │ String  Int64  Int64
─────┼──────────────────────
   1 │ male      690    690
   2 │ female    310    310
```

We are allowed to do this if we drop the grouping columns by passing `keepkeys=false` keyword argument.
Note that for `transform` the key columns would be retained in the produced data frame even with 
`keepkeys=false`; in this case this keyword argument only influences the fact if we check that key columns 
have not changed in this case:

```jldoctest dataframe
julia> using Statistics

julia> select(gd, ["Age"] => cor) # broadcasted to match the number of elements in each group
1000×2 DataFrame
  Row │ Age    Age_cor
      │ Int64  Int64
──────┼────────────────
    1 │    67        1
    2 │    22        1
    3 │    49        1
    4 │    45        1
    5 │    53        1
    6 │    35        1
    7 │    53        1
    8 │    35        1
  ⋮   │   ⋮       ⋮
  994 │    30        1
  995 │    50        1
  996 │    31        1
  997 │    40        1
  998 │    38        1
  999 │    23        1
 1000 │    27        1
       985 rows omitted

julia> transform(groupby(german_ref, :Sex),
                        :Sex => ByRow(uppercase) => :Sex,
                        keepkeys=false)
1000×10 DataFrame
  Row │ id     Age    Sex     Job    Housing  Saving accounts  Checking accoun ⋯
      │ Int64  Int64  String  Int64  String   String           String          ⋯
──────┼─────────────────────────────────────────────────────────────────────────
    1 │     0     67  MALE        2  own      NA               little          ⋯
    2 │     1     22  FEMALE      2  own      little           moderate
    3 │     2     49  MALE        1  own      little           NA
    4 │     3     45  MALE        2  free     little           little
    5 │     4     53  MALE        2  free     little           little          ⋯
    6 │     5     35  MALE        1  free     NA               NA
    7 │     6     53  MALE        2  own      quite rich       NA
    8 │     7     35  MALE        3  rent     little           moderate
  ⋮   │   ⋮      ⋮      ⋮       ⋮       ⋮            ⋮                ⋮        ⋱
  994 │   993     30  MALE        3  own      little           little          ⋯
  995 │   994     50  MALE        2  own      NA               NA
  996 │   995     31  FEMALE      1  own      little           NA
  997 │   996     40  MALE        3  own      little           little
  998 │   997     38  MALE        2  own      little           NA              ⋯
  999 │   998     23  MALE        2  free     little           little
 1000 │   999     27  MALE        2  own      moderate         moderate
                                                  4 columns and 985 rows omitted
```                                               