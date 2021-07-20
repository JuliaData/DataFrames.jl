# Reshaping Data

*DataFrames.jl* is a popular julia library for tabular data which makes easier to read and transform data.
It provides the abstractions of data frames and series, similar to those in *R*, and *Pandas*.

In *DataFrames.jl* data reshaping means the transformaton of the structure of the table or vector (for example 
data frames or series) to make it suitable for further analysis. Let us assume we have a data frame with 
multiindices on the rows and columns. In `DataFrames.jl` currently we have two functions: `stack`, and `unstack`
that we can use for reshaping our data. Their goals are very simple:
- `stack` allows us to go from wide to long data format;
- `unstack` allows us to go from long to wide data format. 

!!! note

  Parts of this section has been taken from [Bogumił Kamiński](https://bkamins.github.io/julialang/2021/05/28/pivot.html)

First we will have a look that how to create multiple indices. There is a simplest way to create a multi index 
data frame object is by passing a list of two or more arrays to the constructor. Have a look to the below example:

```jldoctest dataframe
julia> using DataFrames

julia> grocessary = DataFrame(year = repeat(2019:2021, inner=4),
                                list = repeat(["Sugar", "Soap", "Fruits", "Oil"], outer=3),
                                month=1:12, Date=15:26)
12×4 DataFrame
 Row │ year   list    month  Date
     │ Int64  String  Int64  Int64
─────┼─────────────────────────────
   1 │  2019  Sugar       1     15
   2 │  2019  Soap        2     16
   3 │  2019  Fruits      3     17
   4 │  2019  Oil         4     18
   5 │  2020  Sugar       5     19
   6 │  2020  Soap        6     20
   7 │  2020  Fruits      7     21
   8 │  2020  Oil         8     22
   9 │  2021  Sugar       9     23
  10 │  2021  Soap       10     24
  11 │  2021  Fruits     11     25
  12 │  2021  Oil        12     26

julia> costs = select(grocessary, :year, :list, [:month, :Date] .=> x -> x/2,
                             renamecols=false)
12×4 DataFrame
 Row │ year   list    month    Date
     │ Int64  String  Float64  Float64
─────┼─────────────────────────────────
   1 │  2019  Sugar       0.5      7.5
   2 │  2019  Soap        1.0      8.0
   3 │  2019  Fruits      1.5      8.5
   4 │  2019  Oil         2.0      9.0
   5 │  2020  Sugar       2.5      9.5
   6 │  2020  Soap        3.0     10.0
   7 │  2020  Fruits      3.5     10.5
   8 │  2020  Oil         4.0     11.0
   9 │  2021  Sugar       4.5     11.5
  10 │  2021  Soap        5.0     12.0
  11 │  2021  Fruits      5.5     12.5
  12 │  2021  Oil         6.0     13.0
```

Now before moving further let's practice a bit basic data transformation skills:

```jldoctest dataframe
julia> long_grocessary = stack(grocessary, [:month, :Date], [:year, :list],
                          variable_name=:days, value_name=:grocessary) 
24×4 DataFrame
 Row │ year   list    days    grocessary
     │ Int64  String  String  Int64
─────┼───────────────────────────────────
   1 │  2019  Sugar   month            1
   2 │  2019  Soap    month            2
   3 │  2019  Fruits  month            3
   4 │  2019  Oil     month            4
   5 │  2020  Sugar   month            5
   6 │  2020  Soap    month            6
   7 │  2020  Fruits  month            7
   8 │  2020  Oil     month            8
  ⋮  │   ⋮      ⋮       ⋮         ⋮
  18 │  2020  Soap    Date            20
  19 │  2020  Fruits  Date            21
  20 │  2020  Oil     Date            22
  21 │  2021  Sugar   Date            23
  22 │  2021  Soap    Date            24
  23 │  2021  Fruits  Date            25
  24 │  2021  Oil     Date            26
                           9 rows omitted

julia> long_costs = stack(costs, [:month, :Date], [:year, :list],
                                 variable_name=:days, value_name=:costs)
24×4 DataFrame
 Row │ year   list    days    costs
     │ Int64  String  String  Float64
─────┼────────────────────────────────
   1 │  2019  Sugar   month       0.5
   2 │  2019  Soap    month       1.0
   3 │  2019  Fruits  month       1.5
   4 │  2019  Oil     month       2.0
   5 │  2020  Sugar   month       2.5
   6 │  2020  Soap    month       3.0
   7 │  2020  Fruits  month       3.5
   8 │  2020  Oil     month       4.0
  ⋮  │   ⋮      ⋮       ⋮        ⋮
  18 │  2020  Soap    Date       10.0
  19 │  2020  Fruits  Date       10.5
  20 │  2020  Oil     Date       11.0
  21 │  2021  Sugar   Date       11.5
  22 │  2021  Soap    Date       12.0
  23 │  2021  Fruits  Date       12.5
  24 │  2021  Oil     Date       13.0
                        9 rows omitted

julia> long = innerjoin(long_grocessary, long_costs, on=[:year, :list, :days])
24×5 DataFrame
 Row │ year   list    days    grocessary  costs
     │ Int64  String  String  Int64       Float64
─────┼────────────────────────────────────────────
   1 │  2019  Sugar   month            1      0.5
   2 │  2019  Soap    month            2      1.0
   3 │  2019  Fruits  month            3      1.5
   4 │  2019  Oil     month            4      2.0
   5 │  2020  Sugar   month            5      2.5
   6 │  2020  Soap    month            6      3.0
   7 │  2020  Fruits  month            7      3.5
   8 │  2020  Oil     month            8      4.0
  ⋮  │   ⋮      ⋮       ⋮         ⋮          ⋮
  18 │  2020  Soap    Date            20     10.0
  19 │  2020  Fruits  Date            21     10.5
  20 │  2020  Oil     Date            22     11.0
  21 │  2021  Sugar   Date            23     11.5
  22 │  2021  Soap    Date            24     12.0
  23 │  2021  Fruits  Date            25     12.5
  24 │  2021  Oil     Date            26     13.0
                                    9 rows omitted
```                                           

## The basics of `stack`

The concept of stacking comes in handy when we have data with multi indices. We use the `stack()` function 
to reshape the data frame by converting the data into the stacked form. Reshaping the data from wide to 
long format using the `stack` function means moving (also rotating or pivoting) the innermost column index 
to become the innermost row index.

```
julia> stack(grocessary, 1:3)
36×3 DataFrame
 Row │ Date   variable  value
     │ Int64  String    Any
─────┼────────────────────────
   1 │    15  year      2019
   2 │    16  year      2019
   3 │    17  year      2019
   4 │    18  year      2019
   5 │    19  year      2020
   6 │    20  year      2020
   7 │    21  year      2020
   8 │    22  year      2020
  ⋮  │   ⋮       ⋮        ⋮
  30 │    20  month     6
  31 │    21  month     7
  32 │    22  month     8
  33 │    23  month     9
  34 │    24  month     10
  35 │    25  month     11
  36 │    26  month     12
               21 rows omitted
```

The second optional argument to `stack` indicates the columns to be stacked. These are normally referred 
to as the measured variables. Column names can also be given:

```jldoctest dataframe
julia> stack(grocessary, [:year, :list, :month])
36×3 DataFrame
 Row │ Date   variable  value
     │ Int64  String    Any
─────┼────────────────────────
   1 │    15  year      2019
   2 │    16  year      2019
   3 │    17  year      2019
   4 │    18  year      2019
   5 │    19  year      2020
   6 │    20  year      2020
   7 │    21  year      2020
   8 │    22  year      2020
  ⋮  │   ⋮       ⋮        ⋮
  30 │    20  month     6
  31 │    21  month     7
  32 │    22  month     8
  33 │    23  month     9
  34 │    24  month     10
  35 │    25  month     11
  36 │    26  month     12
               21 rows omitted
```

Note that all columns can be of different types. Type promotion follows the rules of `vcat`.
The stacked `DataFrame` that results includes all of the columns not specified to be stacked. 
These are repeated for each stacked column. These are normally refered to as identifier (id) 
columns. In addition to the id columns, two additional columns labeled `:variable` and `:value` 
contains the column identifier and the stacked columns.

A third optional argument to `stack` represents the id columns that are repeated. This makes it 
easier to specify which variables you want included in the long format:

```jldoctest dataframe
julia> stack(grocessary, [:year, :list], :Date) # first pass `:year` and `:list` variable then `:Date` variable
24×3 DataFrame
 Row │ Date   variable  value
     │ Int64  String    Any
─────┼─────────────────────────
   1 │    15  year      2019
   2 │    16  year      2019
   3 │    17  year      2019
   4 │    18  year      2019
   5 │    19  year      2020
   6 │    20  year      2020
   7 │    21  year      2020
   8 │    22  year      2020
  ⋮  │   ⋮       ⋮        ⋮
  18 │    20  list      Soap
  19 │    21  list      Fruits
  20 │    22  list      Oil
  21 │    23  list      Sugar
  22 │    24  list      Soap
  23 │    25  list      Fruits
  24 │    26  list      Oil
                 9 rows omitted
```

To make a view add `view=true` keyword argument; in that case columns of the resulting data frame share 
memory with columns of the source data frame, so the operation is potentially unsafe.

```jldoctest dataframe
julia> stack(grocessary, ["year", "list"], "Date", variable_name="key", value_name="data") # optionally we can rename columns
24×3 DataFrame
 Row │ Date   key     data
     │ Int64  String  Any
─────┼───────────────────────
   1 │    15  year    2019
   2 │    16  year    2019
   3 │    17  year    2019
   4 │    18  year    2019
   5 │    19  year    2020
   6 │    20  year    2020
   7 │    21  year    2020
   8 │    22  year    2020
  ⋮  │   ⋮      ⋮       ⋮
  18 │    20  list    Soap
  19 │    21  list    Fruits
  20 │    22  list    Oil
  21 │    23  list    Sugar
  22 │    24  list    Soap
  23 │    25  list    Fruits
  24 │    26  list    Oil
               9 rows omitted
```

if second argument is omitted in `stack` , all other columns are assumed to be the id-variables:

```jldoctest dataframe
julia> stack(grocessary, Not([:Date, :month]))
24×4 DataFrame
 Row │ month  Date   variable  value
     │ Int64  Int64  String    Any
─────┼────────────────────────────────
   1 │     1     15  year      2019
   2 │     2     16  year      2019
   3 │     3     17  year      2019
   4 │     4     18  year      2019
   5 │     5     19  year      2020
   6 │     6     20  year      2020
   7 │     7     21  year      2020
   8 │     8     22  year      2020
  ⋮  │   ⋮      ⋮       ⋮        ⋮
  18 │     6     20  list      Soap
  19 │     7     21  list      Fruits
  20 │     8     22  list      Oil
  21 │     9     23  list      Sugar
  22 │    10     24  list      Soap
  23 │    11     25  list      Fruits
  24 │    12     26  list      Oil
                        9 rows omitted
```

We can use index instead of symbols:

```jldoctest dataframe
julia> stack(grocessary, Not([3, 4]))
24×4 DataFrame
 Row │ month  Date   variable  value
     │ Int64  Int64  String    Any
─────┼────────────────────────────────
   1 │     1     15  year      2019
   2 │     2     16  year      2019
   3 │     3     17  year      2019
   4 │     4     18  year      2019
   5 │     5     19  year      2020
   6 │     6     20  year      2020
   7 │     7     21  year      2020
   8 │     8     22  year      2020
  ⋮  │   ⋮      ⋮       ⋮        ⋮
  18 │     6     20  list      Soap
  19 │     7     21  list      Fruits
  20 │     8     22  list      Oil
  21 │     9     23  list      Sugar
  22 │    10     24  list      Soap
  23 │    11     25  list      Fruits
  24 │    12     26  list      Oil
                        9 rows omitted
```

## Basics of `unstack`

Now, if we do inverse operation then it is known as unstacking. It means moving the innermost row index to 
become the innermost column index.                                                                         

We will reshape the data from long to wide using `unstack()` function:

```jldoctest dataframe
julia> grocessary = DataFrame(year = repeat(2019:2021, inner=4),
                                list = repeat(["Sugar", "Soap", "Fruits", "Oil"], outer=3),
                                month=1:12, Date=15:26)
12×4 DataFrame
 Row │ year   list    month  Date
     │ Int64  String  Int64  Int64
─────┼─────────────────────────────
   1 │  2019  Sugar       1     15
   2 │  2019  Soap        2     16
   3 │  2019  Fruits      3     17
   4 │  2019  Oil         4     18
   5 │  2020  Sugar       5     19
   6 │  2020  Soap        6     20
   7 │  2020  Fruits      7     21
   8 │  2020  Oil         8     22
   9 │  2021  Sugar       9     23
  10 │  2021  Soap       10     24
  11 │  2021  Fruits     11     25
  12 │  2021  Oil        12     26

julia> grocessary1 = stack(grocessary, [:year, :list, :month])
36×3 DataFrame
 Row │ Date   variable  value
     │ Int64  String    Any
─────┼────────────────────────
   1 │    15  year      2019
   2 │    16  year      2019
   3 │    17  year      2019
   4 │    18  year      2019
   5 │    19  year      2020
   6 │    20  year      2020
   7 │    21  year      2020
   8 │    22  year      2020
  ⋮  │   ⋮       ⋮        ⋮
  30 │    20  month     6
  31 │    21  month     7
  32 │    22  month     8
  33 │    23  month     9
  34 │    24  month     10
  35 │    25  month     11
  36 │    26  month     12
               21 rows omitted

julia> grocessary_ref = unstack(grocessary1) # here we got the original data frame
12×4 DataFrame
 Row │ Date   year  list    month
     │ Int64  Any   Any     Any
─────┼────────────────────────────
   1 │    15  2019  Sugar   1
   2 │    16  2019  Soap    2
   3 │    17  2019  Fruits  3
   4 │    18  2019  Oil     4
   5 │    19  2020  Sugar   5
   6 │    20  2020  Soap    6
   7 │    21  2020  Fruits  7
   8 │    22  2020  Oil     8
   9 │    23  2021  Sugar   9
  10 │    24  2021  Soap    10
  11 │    25  2021  Fruits  11
  12 │    26  2021  Oil     12

julia> df = unstack(grocessary_ref, :year, :list, :month) # we did the unstack with specified keys
3×5 DataFrame
 Row │ year  Sugar  Soap  Fruits  Oil
     │ Any   Any    Any   Any     Any
─────┼────────────────────────────────
   1 │ 2019  1      2     3       4
   2 │ 2020  5      6     7       8
   3 │ 2021  9      10    11      12

julia> df1 = unstack(grocessary1, renamecols=n->string("unstacked_", n)) # we renamed the unstacked columns
12×4 DataFrame
 Row │ Date   unstacked_year  unstacked_list  unstacked_month
     │ Int64  Any             Any             Any
─────┼────────────────────────────────────────────────────────
   1 │    15  2019            Sugar           1
   2 │    16  2019            Soap            2
   3 │    17  2019            Fruits          3
   4 │    18  2019            Oil             4
   5 │    19  2020            Sugar           5
   6 │    20  2020            Soap            6
   7 │    21  2020            Fruits          7
   8 │    22  2020            Oil             8
   9 │    23  2021            Sugar           9
  10 │    24  2021            Soap            10
  11 │    25  2021            Fruits          11
  12 │    26  2021            Oil             12
```

Lets dive deep in `unstack`:

```jldoctest dataframe
julia> long = innerjoin(long_grocessary, long_costs, on=[:year, :list, :days]);
```

Assume we want to get the grocessary_ref table back. We need to unstack our long table putting `:year` and 
`:list` in rows and `:days` in columns, while taking `:grocessary` as values:

julia> unstack(long, [:year, :list], :days, :grocessary)
12×4 DataFrame
 Row │ year   list    month   Date
     │ Int64  String  Int64?  Int64?
─────┼───────────────────────────────
   1 │  2019  Sugar        1      15
   2 │  2019  Soap         2      16
   3 │  2019  Fruits       3      17
   4 │  2019  Oil          4      18
   5 │  2020  Sugar        5      19
   6 │  2020  Soap         6      20
   7 │  2020  Fruits       7      21
   8 │  2020  Oil          8      22
   9 │  2021  Sugar        9      23
  10 │  2021  Soap        10      24
  11 │  2021  Fruits      11      25
  12 │  2021  Oil         12      26
```

We can also check that whatever we have got that is same as *grocessary* or not:

```jldoctest dataframe
julia> unstack(long, [:year, :list], :days, :grocessary) == grocessary
true
```

now we will put only `:year` in rows after dropping `:list` and we will see what we get:

```jldoctest dataframe
julia> unstack(long, :year, :list, :grocessary)
ERROR: ArgumentError: Duplicate entries in unstack at row 13 for key (2019,) and variable Sugar. Pass allowduplicates=true to allow them.

julia> unstack(long, :year, :list, :grocessary, allowduplicates = true)
3×5 DataFrame
 Row │ year   Sugar   Soap    Fruits  Oil
     │ Int64  Int64?  Int64?  Int64?  Int64?
─────┼───────────────────────────────────────
   1 │  2019      15      16      17      18
   2 │  2020      19      20      21      22
   3 │  2021      23      24      25      26
```

Clearly we can see even if we pass `allowduplicates=true` then we do not geet our desired result. 
This leads us to the first case.

### Pivot tables with `unstack`

Most likely we want to aggregate grocessary per year using the `sum` function. This is a classic pivot 
table task. In DataFrames.jl currently one does it in two steps: first aggregate, then reshape. Here 
is how we can do it (We are showing two separate steps, but you could use e.g. [Chain.jl](https://github.com/jkrumbiegel/Chain.jl) to streamline the processing):

```jldoctest dataframe
julia> df = combine(groupby(long, [:year, :list]), :grocessary => sum => :grocessary)
12×3 DataFrame
 Row │ year   list    grocessary
     │ Int64  String  Int64
─────┼───────────────────────────
   1 │  2019  Sugar           16
   2 │  2019  Soap            18
   3 │  2019  Fruits          20
   4 │  2019  Oil             22
   5 │  2020  Sugar           24
   6 │  2020  Soap            26
   7 │  2020  Fruits          28
   8 │  2020  Oil             30
   9 │  2021  Sugar           32
  10 │  2021  Soap            34
  11 │  2021  Fruits          36
  12 │  2021  Oil             38

julia> unstack(df, :year, :list, :grocessary)
3×5 DataFrame
 Row │ year   Sugar   Soap    Fruits  Oil
     │ Int64  Int64?  Int64?  Int64?  Int64?
─────┼───────────────────────────────────────
   1 │  2019      16      18      20      22
   2 │  2020      24      26      28      30
   3 │  2021      32      34      36      38
```
