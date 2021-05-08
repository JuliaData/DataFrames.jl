# First Steps with DataFrames

The first and important part to work with DataFrames.jl, is to install the `DataFrames`
package which is available thorugh the Julia package system and can be installed using the
following commands:

```julia
julia> using Pkg
julia> Pkg.add("DataFrames")
```

Or,

```julia
julia> ] # `]` should be pressed 

(@v1.6) pkg>  add DataFrames
```

To make sure everything works as expected, try to load the package and if you have the time execute
its test suits. It can take around 2726.057766 seconds (770.99 k allocations: 59.873 MiB, 0.07% gc time).
```julia
julia> using DataFrames
julia> using Pkg
julia> Pkg.test("DataFrames")
```

Additionally, you must check the version installed with `st` command and ensure it is `1.0` release.
```julia
julia> ]

(@v1.6) pkg> st 
```

Throughout the rest of the tutorial we will assume that you have installed DataFrames.jl package and
have already typed `using DataFrames`.
Let's get started by loading the DataFrames package:
```jldoctest
julia> using DataFrames
```

The object of the `DataFrame` that each row is an observation and each column is a feature or something similar. 
The reason is that `DataFrame` is not a collection of its columns.

# Constructors and basic utility functions

## Constructors

In this section you will see many ways to create a `DataFrame` using the `DataFrame(...)` constructor.
First, let's create an empty `DataFrame`:
```jldoctest dataframe
julia> using DataFrames

julia> df = DataFrame()
0×0 DataFrame
```

Or, we could call the constructor using keyword arguments to add columns to the `DataFrame`:
```jldoctest
julia> using DataFrames

julia> df = DataFrame(A=1:3, B=5:7, fixed=1)
3×3 DataFrame
 Row │ A      B      fixed
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      5      1
   2 │     2      6      1
   3 │     3      7      1
```
note in column `:const` that scalars get automatically broadcasted.

We assume that you will have installed `DataFrames.jl`. If you haven't install `CSV.jl` in your environment, 
run the REPL (command-line interface started usually by typing `julia`). Run:
```julia
julia> using Pkg
julia> Pkg.add("CSV")
```
Now before moving further make sure you have the  correct versions of `CSV.jl` and `DataFrames.jl` 
which are `CSV v0.8.4` and `DataFrames v1.0.0` respectively.

Now, we will explore how to load a CSV file into a `DataFrame`. Unlike Python's Pandas `read_csv`
the functions in Julia are separated into two packages - `CSV.jl` and `DataFrames.jl`. As the first
step, you have to declare the libraries you will use. In our case `CSV` and `DataFrames`. In order to
turn the `CSV.read` to a DataFrame you have to pass it to the `DataFrames.DataFrame` object. You can
wrap DataFrame around the `CSV.read(path; kwargs)`.

As the first step, we have to declare the libraries we will use. In our case `CSV` and `DataFrames`.
```jldoctest dataframe
julia> using DataFrames

julia> using CSV

julia> german_ref = CSV.read((joinpath(dirname(pathof(DataFrames)),
                                 "..", "docs", "src", "assets", "german.csv")),
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

julia> german = copy(german_ref); # It will copy the data frame
```

Now let's talk about the given code block:
```julia
german = CSV.read((joinpath(dirname(pathof(DataFrames)),
                                 "..", "docs", "src", "assets", "german.csv")),
                       DataFrame)
```
- we are storing `german.csv` file in the `DataFrames.jl` repository to make user's life easier and 
  avoid having to download it each time;
- `pathof(DataFrames)` gives us the full path of the file that was used to import `DataFrames`;
- first we split the directory part from it using `dirname`;
- then from this directory we need to move to the directory where `german.csv` is stored; we use 
  `joinpath` as this is a recommended way to compose paths to resources stored on disk in an operating 
  system independent way  (remember that Widnows and Unix differ as they use either `/` or `\` as path 
  separator - `joinpath` is a function to make sure we are not running into issues with this);
- then we read the CSV file; the second argument to `CSV.read` is `DataFrame` to indicate that we want to 
  read in the file into a `DataFrame` (as `CSV.read` allows for many different target formats of data it 
  can read-into).

You can see that Julia representation (unlike Python's Pandas) displays the data type of the column,
In our case it is a `Int64`, and `String`.

To access the columns directly (i.e. without copying) you can use `german.Sex`, `german."Sex"`,
`german[!, :Sex]` or `german[!, "Sex"]`. The two latter syntaxes are more flexible as they allow
us passing a variable holding the name of the column, and not only a literal name. Columns name
can be passed either as a `Symbol` (written as `:Sex`, `:var"Sex"` or `Symbol("Sex")`) or as a string 
(written as `"Sex"`). Variable interpolation into the string using `$` does not work if you have forms
like `german."Sex"` and `:var"Sex"`. You can also access the column using an integer index specifying 
their position.

```jldoctest dataframe
julia> german[!, :Sex]
1000-element PooledArrays.PooledVector{String, UInt32, Vector{UInt32}}:
 "male"
 "female"
 "male"
 "male"
 "male"
 "male"
 "male"
 "male"
 "male"
 "male"
 ⋮
 "male"
 "male"
 "male"
 "male"
 "female"
 "male"
 "male"
 "male"
 "male"
```

Since `german[!, :Sex]` does not make a copy, changing the elements of the column vector returned by this
sysntax will affect the values stored in the original `german` dataframe. To get a **copy** of the column you can use
`german[:, :Sex]`: changing the vector returned by this syntax does not change `german`.

```jldoctest dataframe
julia> german.Sex
1000-element PooledArrays.PooledVector{String, UInt32, Vector{UInt32}}:
 "male"
 "female"
 "male"
 "male"
 "male"
 "male"
 "male"
 "male"
 "male"
 "male"
 ⋮
 "male"
 "male"
 "male"
 "male"
 "female"
 "male"
 "male"
 "male"
 "male"

julia> german.Sex === german[!, :Sex]
true

julia> german.Sex === german[:, :Sex]
false
```

You can obtain the column names as strings using the `names` function:
```jldoctest dataframe
julia> names(german)
10-element Vector{String}:
 "id"
 "Age"
 "Sex"
 "Job"
 "Housing"
 "Saving accounts"
 "Checking account"
 "Credit amount"
 "Duration"
 "Purpose"
```

You can also get column names with a given `eltype` (element type):
```jldoctest dataframe
julia> names(german, String)
5-element Vector{String}:
 "Sex"
 "Housing"
 "Saving accounts"
 "Checking account"
 "Purpose"
```

To get column names as `Symbol`s use the `propertynames` function:

```jldoctest dataframe
julia> propertynames(german)
10-element Vector{Symbol}:
 :id
 :Age
 :Sex
 :Job
 :Housing
 Symbol("Saving accounts")
 Symbol("Checking account")
 Symbol("Credit amount")
 :Duration
 :Purpose
```

using `eltype` on `eachcol(german)` returns element types of columns:
```jldoctest dataframe
julia> eltype.(eachcol(german))
10-element Vector{DataType}:
 Int64
 Int64
 String
 Int64
 String
 String
 String
 Int64
 Int64
 String
```

!!! note

  DataFrames.jl allows to use `Symbol`s (like `:id`) and strings (like `"id"`)
  for all column indexing operations for convenience. However, using `Symbol`s
  is slightly faster and should generally be preferred, if not generating them
  via string manipulation.

To remove all rows and columns from a DataFrame you can use `empty` and `empty!` functions to remove 
all rows from a `DataFrame`:
```jldoctest dataframe
julia> empty(german)
0×10 DataFrame

julia> german
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

julia> empty!(german)
0×10 DataFrame

julia> german
0×10 DataFrame
```

In the above example `empty` function create a new `DataFrame` with the same column names 
and column element types as `german` but with zero rows. On the other hand `empty!` function 
remove all rows from `german` and make each of its column empty. 

## Getting basic information about a data frame

In this section we will learn about how to get basic information on our `german` `DataFrame`:

The standard `size` function works to get dimensions of the data frame,
```jldoctest dataframe
julia> german = copy(german_ref);
```

```jldoctest dataframe
julia> size(german)
(1000, 10)

julia> size(german, 1)
1000

julia> size(german, 2)
10
```

Additionally the `nrow` and `ncol` functions can be used to get the number of rows and columns in a data frame:
```jldoctest dataframe
julia> nrow(german)
1000

julia> ncol(german)
10
```

To get basic statistics of data in your data frame use the `describe` function (check out the help of `describe` 
for information on how to customize shown statistics).
```jldoctest dataframe
julia> describe(german)
10×7 DataFrame
 Row │ variable          mean     min       median  max              nmissing  ⋯
     │ Symbol            Union…   Any       Union…  Any              Int64     ⋯
─────┼──────────────────────────────────────────────────────────────────────────
   1 │ id                499.5    0         499.5   999                     0  ⋯
   2 │ Age               35.546   19        33.0    75                      0
   3 │ Sex                        female            male                    0
   4 │ Job               1.904    0         2.0     3                       0
   5 │ Housing                    free              rent                    0  ⋯
   6 │ Saving accounts            NA                rich                    0
   7 │ Checking account           NA                rich                    0
   8 │ Credit amount     3271.26  250       2319.5  18424                   0
   9 │ Duration          20.903   4         18.0    72                      0  ⋯
  10 │ Purpose                    business          vacation/others         0
                                                                1 column omitted
```

To limit the columns shown by `desribe` use `cols` keyword argument:
```jldoctest dataframe
julia> describe(german, cols=1:3)
3×7 DataFrame
 Row │ variable  mean    min     median  max   nmissing  eltype
     │ Symbol    Union…  Any     Union…  Any   Int64     DataType
─────┼────────────────────────────────────────────────────────────
   1 │ id        499.5   0       499.5   999          0  Int64
   2 │ Age       35.546  19      33.0    75           0  Int64
   3 │ Sex               female          male         0  String
```

The default statistics reported are mean, min, median, max, number of missing values, and element type of 
the column. If columns contain `missing` values they are skipped when computing the summary statistics.

You can adjust printing options by calling the `show` function manually: `show(german, allrows=true)`
prints all rows even if they do not fit on screen and `show(german, allcols=true)` does the same for
columns.

```jldoctest dataframe
julia> show(german, allcols=true)
1000×10 DataFrame
  Row │ id     Age    Sex     Job    Housing  Saving accounts  Checking account  Credit amount  Duration  Purpose
      │ Int64  Int64  String  Int64  String   String           String            Int64          Int64     String
──────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
    1 │     0     67  male        2  own      NA               little                     1169         6  radio/TV
    2 │     1     22  female      2  own      little           moderate                   5951        48  radio/TV
    3 │     2     49  male        1  own      little           NA                         2096        12  education
    4 │     3     45  male        2  free     little           little                     7882        42  furniture/equipment
    5 │     4     53  male        2  free     little           little                     4870        24  car
    6 │     5     35  male        1  free     NA               NA                         9055        36  education
    7 │     6     53  male        2  own      quite rich       NA                         2835        24  furniture/equipment
    8 │     7     35  male        3  rent     little           moderate                   6948        36  car
  ⋮   │   ⋮      ⋮      ⋮       ⋮       ⋮            ⋮                ⋮                ⋮           ⋮               ⋮
  994 │   993     30  male        3  own      little           little                     3959        36  furniture/equipment
  995 │   994     50  male        2  own      NA               NA                         2390        12  car
  996 │   995     31  female      1  own      little           NA                         1736        12  furniture/equipment
  997 │   996     40  male        3  own      little           little                     3857        30  car
  998 │   997     38  male        2  own      little           NA                          804        12  radio/TV
  999 │   998     23  male        2  free     little           little                     1845        45  radio/TV
 1000 │   999     27  male        2  own      moderate         moderate                   4576        45  car
                                                                                                              985 rows omitted
```

You can also compute descriptive statistics directly on individual columns:

```jldoctest dataframe
julia> using Statistics

julia> mean(german.Age)
35.546
```

`mapcols` function return a `DataFrame` where each column of `german` is transformed using function `f`.
Note that `mapcols` guarantees not to reuse the columns from `german` in the returned `DataFrame`. If `f` 
returns its argument then it gets copied before being stored. 

```jldoctest dataframe
julia> german = copy(german_ref);

julia> mapcols(id -> id.^2, german)
1000×10 DataFrame
  Row │ id      Age    Sex           Job    Housing   Saving accounts       Ch ⋯
      │ Int64   Int64  String        Int64  String    String                St ⋯
──────┼─────────────────────────────────────────────────────────────────────────
    1 │      0   4489  malemale          4  ownown    NANA                  li ⋯
    2 │      1    484  femalefemale      4  ownown    littlelittle          mo
    3 │      4   2401  malemale          1  ownown    littlelittle          NA
    4 │      9   2025  malemale          4  freefree  littlelittle          li
    5 │     16   2809  malemale          4  freefree  littlelittle          li ⋯
    6 │     25   1225  malemale          1  freefree  NANA                  NA
    7 │     36   2809  malemale          4  ownown    quite richquite rich  NA
    8 │     49   1225  malemale          9  rentrent  littlelittle          mo
  ⋮   │   ⋮       ⋮         ⋮          ⋮       ⋮               ⋮               ⋱
  994 │ 986049    900  malemale          9  ownown    littlelittle          li ⋯
  995 │ 988036   2500  malemale          4  ownown    NANA                  NA
  996 │ 990025    961  femalefemale      1  ownown    littlelittle          NA
  997 │ 992016   1600  malemale          9  ownown    littlelittle          li
  998 │ 994009   1444  malemale          4  ownown    littlelittle          NA ⋯
  999 │ 996004    529  malemale          4  freefree  littlelittle          li
 1000 │ 998001    729  malemale          4  ownown    moderatemoderate      mo
                                                  4 columns and 985 rows omitted
```

If you want to look at first and last rows of a data frame (respectively) then you can do this
using `first` and `last` functions:

```jldoctest dataframe
julia> german = copy(german_ref);

julia> first(german, 6)
6×10 DataFrame
 Row │ id     Age    Sex     Job    Housing  Saving accounts  Checking account ⋯
     │ Int64  Int64  String  Int64  String   String           String           ⋯
─────┼──────────────────────────────────────────────────────────────────────────
   1 │     0     67  male        2  own      NA               little           ⋯
   2 │     1     22  female      2  own      little           moderate
   3 │     2     49  male        1  own      little           NA
   4 │     3     45  male        2  free     little           little
   5 │     4     53  male        2  free     little           little           ⋯
   6 │     5     35  male        1  free     NA               NA
                                                               3 columns omitted

julia> last(german, 5)
5×10 DataFrame
 Row │ id     Age    Sex     Job    Housing  Saving accounts  Checking account ⋯
     │ Int64  Int64  String  Int64  String   String           String           ⋯
─────┼──────────────────────────────────────────────────────────────────────────
   1 │   995     31  female      1  own      little           NA               ⋯
   2 │   996     40  male        3  own      little           little
   3 │   997     38  male        2  own      little           NA
   4 │   998     23  male        2  free     little           little
   5 │   999     27  male        2  own      moderate         moderate         ⋯
                                                               3 columns omitted
```

Using `first` and `last` without number of rows will return a first/last `DataFrameRow` in the data frame.
`DataFrameRow` is a view into a single row of an `AbstractDataFrame`. It stores only a reference to a parent 
`DataFrame` and information about which row and columns from the parent are selected (both as integer indices 
referring to the parent). 

```jldoctest dataframe
julia> first(german)
DataFrameRow
 Row │ id     Age    Sex     Job    Housing  Saving accounts  Checking account ⋯
     │ Int64  Int64  String  Int64  String   String           String           ⋯
─────┼──────────────────────────────────────────────────────────────────────────
   1 │     0     67  male        2  own      NA               little           ⋯
                                                               3 columns omitted

julia> last(german)
DataFrameRow
  Row │ id     Age    Sex     Job    Housing  Saving accounts  Checking accoun ⋯
      │ Int64  Int64  String  Int64  String   String           String          ⋯
──────┼─────────────────────────────────────────────────────────────────────────
 1000 │   999     27  male        2  own      moderate         moderate        ⋯
                                                               4 columns omitted
```

## Taking a Subset of `german` data frame

### Indexing Syntax

Specific subsets of a data frame can be extracted using the indexing syntax, similar to matrices.
In the [Indexing](https://dataframes.juliadata.org/stable/lib/indexing/#Indexing) section of the
manual you can find all details about the available options. Here we highlight the basic ones.

The colon `:` indicates that all items (rows or columns depending on its position) should be retained:

```jldoctest dataframe
julia> german[1:5, :]
5×10 DataFrame
 Row │ id     Age    Sex     Job    Housing  Saving accounts  Checking account ⋯
     │ Int64  Int64  String  Int64  String   String           String           ⋯
─────┼──────────────────────────────────────────────────────────────────────────
   1 │     0     67  male        2  own      NA               little           ⋯
   2 │     1     22  female      2  own      little           moderate
   3 │     2     49  male        1  own      little           NA
   4 │     3     45  male        2  free     little           little
   5 │     4     53  male        2  free     little           little           ⋯
                                                               3 columns omitted

julia> german[[1, 6, 15], :]
3×10 DataFrame
 Row │ id     Age    Sex     Job    Housing  Saving accounts  Checking account ⋯
     │ Int64  Int64  String  Int64  String   String           String           ⋯
─────┼──────────────────────────────────────────────────────────────────────────
   1 │     0     67  male        2  own      NA               little           ⋯
   2 │     5     35  male        1  free     NA               NA
   3 │    14     28  female      2  rent     little           little
                                                               3 columns omitted

julia> german[:, [:Age, :Sex]]
1000×2 DataFrame
  Row │ Age    Sex
      │ Int64  String
──────┼───────────────
    1 │    67  male
    2 │    22  female
    3 │    49  male
    4 │    45  male
    5 │    53  male
    6 │    35  male
    7 │    53  male
    8 │    35  male
  ⋮   │   ⋮      ⋮
  994 │    30  male
  995 │    50  male
  996 │    31  female
  997 │    40  male
  998 │    38  male
  999 │    23  male
 1000 │    27  male
      985 rows omitted

julia> german[1:5, [:Sex, :Age]]
5×2 DataFrame
 Row │ Sex     Age
     │ String  Int64
─────┼───────────────
   1 │ male       67
   2 │ female     22
   3 │ male       49
   4 │ male       45
   5 │ male       53
```

Pay attention that `german[!, [:Sex]]` and `german[:, [:Sex]]` return a data frame object,
while `german[!, :Sex]` and `german[:, :Sex]` return a vector:

```jldoctest dataframe
julia> german[!, [:Sex]]
1000×1 DataFrame
  Row │ Sex
      │ String
──────┼────────
    1 │ male
    2 │ female
    3 │ male
    4 │ male
    5 │ male
    6 │ male
    7 │ male
    8 │ male
  ⋮   │   ⋮
  994 │ male
  995 │ male
  996 │ female
  997 │ male
  998 │ male
  999 │ male
 1000 │ male
985 rows omitted

julia> german[!, [:Sex]] == german[:, [:Sex]]
true

julia> german[!, :Sex]
1000-element PooledArrays.PooledVector{String, UInt32, Vector{UInt32}}:
 "male"
 "female"
 "male"
 "male"
 "male"
 "male"
 "male"
 "male"
 "male"
 "male"
 ⋮
 "male"
 "male"
 "male"
 "male"
 "female"
 "male"
 "male"
 "male"
 "male"

julia> german[!, :Sex] == german[:, :Sex]
true
```

In the first case, `[:Sex]` is a vector, indicating that the resulting object should be a data frame.
On the other hand, `:Sex` is a single symbol, indicating that a single column vector should be extracted.
Note that in the first case a vector is required to be passed (not just any iterable),
so e.g. `german[:, (:Age, :Sex)]` is not allowed, but `german[:, [:Age, :Sex]]` is valid.

### Most elementary `get` and `set` operations
Here, `get` means you retrieve the column, and `set` means you put the column in the data frame.

Given the data frame `german` earlier we have created, here are various ways to grab one of its columns as vector:
```jldoctest dataframe
julia> german
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

julia> german.Age # all get the vector stored in our DataFrame without copying it
1000-element Vector{Int64}:
 67
 22
 49
 45
 53
 35
 53
 35
 61
 28
  ⋮
 34
 23
 30
 50
 31
 40
 38
 23
 27

julia> german."Sex" # the same using string indexing
1000-element PooledArrays.PooledVector{String, UInt32, Vector{UInt32}}:
 "male"
 "female"
 "male"
 "male"
 "male"
 "male"
 "male"
 "male"
 "male"
 "male"
 ⋮
 "male"
 "male"
 "male"
 "male"
 "female"
 "male"
 "male"
 "male"
 "male"

julia> german[:, 3] # note that this creates a copy
1000-element PooledArrays.PooledVector{String, UInt32, Vector{UInt32}}:
 "male"
 "female"
 "male"
 "male"
 "male"
 "male"
 "male"
 "male"
 "male"
 "male"
 ⋮
 "male"
 "male"
 "male"
 "male"
 "female"
 "male"
 "male"
 "male"
 "male"
```

To grab one row as `DataFrame`, we can index as follows:
```jldoctest dataframe
julia> german[2:2, :]
1×10 DataFrame
 Row │ id     Age    Sex     Job    Housing  Saving accounts  Checking account ⋯
     │ Int64  Int64  String  Int64  String   String           String           ⋯
─────┼──────────────────────────────────────────────────────────────────────────
   1 │     1     22  female      2  own      little           moderate         ⋯
                                                               3 columns omitted

julia> german[3, :] # this produces a DataFrameRow which is treated as 1-dimensional object similar to a NamedTuple
DataFrameRow
 Row │ id     Age    Sex     Job    Housing  Saving accounts  Checking account ⋯
     │ Int64  Int64  String  Int64  String   String           String           ⋯
─────┼──────────────────────────────────────────────────────────────────────────
   3 │     2     49  male        1  own      little           NA               ⋯
                                                               3 columns omitted
```

we can grab a single cell or element with the same syntax to grab an element of an array:
```jldoctest dataframe
julia> german[4, 4]
2
```

or to get a new DataFrame that is subset of rows and columns:
```jldoctest dataframe
julia> german[4:5, 4:5]
2×2 DataFrame
 Row │ Job    Housing
     │ Int64  String
─────┼────────────────
   1 │     2  free
   2 │     2  free
```

Assignment of a scalar to a data frame can be done in ranges using broadcasting:
```jldoctest dataframe
julia> german[1:2, 1:2] .= 1
2×2 SubDataFrame
 Row │ id     Age
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     1      1

julia> german
1000×10 DataFrame
  Row │ id     Age    Sex     Job    Housing  Saving accounts  Checking accoun ⋯
      │ Int64  Int64  String  Int64  String   String           String          ⋯
──────┼─────────────────────────────────────────────────────────────────────────
    1 │     1      1  male        2  own      NA               little          ⋯
    2 │     1      1  female      2  own      little           moderate
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

The indexing syntax can also be used to select rows based on conditions on variables:
```jldoctest dataframe
julia> german[german.id .> 600, :]
399×10 DataFrame
 Row │ id     Age    Sex     Job    Housing  Saving accounts  Checking account ⋯
     │ Int64  Int64  String  Int64  String   String           String           ⋯
─────┼──────────────────────────────────────────────────────────────────────────
   1 │   601     30  female      2  own      little           moderate         ⋯
   2 │   602     34  female      1  free     little           moderate
   3 │   603     28  female      3  own      little           NA
   4 │   604     23  female      2  own      little           rich
   5 │   605     22  male        2  own      quite rich       little           ⋯
   6 │   606     74  male        3  own      little           NA
   7 │   607     50  female      2  free     moderate         moderate
   8 │   608     33  male        2  own      little           NA
  ⋮  │   ⋮      ⋮      ⋮       ⋮       ⋮            ⋮                ⋮         ⋱
 393 │   993     30  male        3  own      little           little           ⋯
 394 │   994     50  male        2  own      NA               NA
 395 │   995     31  female      1  own      little           NA
 396 │   996     40  male        3  own      little           little
 397 │   997     38  male        2  own      little           NA               ⋯
 398 │   998     23  male        2  free     little           little
 399 │   999     27  male        2  own      moderate         moderate
                                                  3 columns and 384 rows omitted
```

If you need to match a specific subset of values, then `in()` can be applied:
```jldoctest dataframe
julia> german[in.(german.id, Ref(Set([1, 6, 908, 955]))), :]
5×10 DataFrame
 Row │ id     Age    Sex     Job    Housing  Saving accounts  Checking account ⋯
     │ Int64  Int64  String  Int64  String   String           String           ⋯
─────┼──────────────────────────────────────────────────────────────────────────
   1 │     1      1  male        2  own      NA               little           ⋯
   2 │     1      1  female      2  own      little           moderate
   3 │     6     53  male        2  own      quite rich       NA
   4 │   908     46  female      1  own      little           NA
   5 │   955     57  female      3  rent     rich             little           ⋯
                                                               3 columns omitted
```

Equivalently, the `in` function can be called with a single argument to create a function object that
tests whether each value belongs to the subset (partial application of `in`):
`german[in(Set([1, 6, 908, 955])).(german.id), :]`

## Not, Between, Cols, findall and All selectors

Finally, you can use `Not`, `Between`, `Cols`, and `All` selectors in more complex column selection
scenarioes (note that `Cols()` selects no columns while `All()` selects all columns). Now we will 
see how `Cols` and `Between` can be used to select columns of a data frame. `All()` allow us to select 
all columns of `DataFrame` while `Between` selector allow us to specify a range of columns (we can 
specify the start and stop column using any of the single column selector syntaxes). On the other hand, 
`Not` selector allow us to specify the columns we want to exclude from the resulting data frames. We 
can put any valid other column selector inside `Not`. Meanwhile `Cols()` selector pick a union of other 
selectors passed as its arguments.

A `Not` selector (from the [InvertedIndices](https://github.com/mbauman/InvertedIndices.jl) package) 
can be used to select all columns excluding a specific subset:

```jldoctest dataframe
julia> german[:, Not(:Age)] # `:Age` was after `:id`
1000×9 DataFrame
  Row │ id     Sex     Job    Housing  Saving accounts  Checking account  Cred ⋯
      │ Int64  String  Int64  String   String           String            Int6 ⋯
──────┼─────────────────────────────────────────────────────────────────────────
    1 │     1  male        2  own      NA               little                 ⋯
    2 │     1  female      2  own      little           moderate
    3 │     2  male        1  own      little           NA
    4 │     3  male        2  free     little           little
    5 │     4  male        2  free     little           little                 ⋯
    6 │     5  male        1  free     NA               NA
    7 │     6  male        2  own      quite rich       NA
    8 │     7  male        3  rent     little           moderate
  ⋮   │   ⋮      ⋮       ⋮       ⋮            ⋮                ⋮               ⋱
  994 │   993  male        3  own      little           little                 ⋯
  995 │   994  male        2  own      NA               NA
  996 │   995  female      1  own      little           NA
  997 │   996  male        3  own      little           little
  998 │   997  male        2  own      little           NA                     ⋯
  999 │   998  male        2  free     little           little
 1000 │   999  male        2  own      moderate         moderate
                                                  3 columns and 985 rows omitted
```

```jldoctest dataframe
julia> german[:, Between(:Sex, :Housing)]
1000×3 DataFrame
  Row │ Sex     Job    Housing
      │ String  Int64  String
──────┼────────────────────────
    1 │ male        2  own
    2 │ female      2  own
    3 │ male        1  own
    4 │ male        2  free
    5 │ male        2  free
    6 │ male        1  free
    7 │ male        2  own
    8 │ male        3  rent
  ⋮   │   ⋮       ⋮       ⋮
  994 │ male        3  own
  995 │ male        2  own
  996 │ female      1  own
  997 │ male        3  own
  998 │ male        2  own
  999 │ male        2  free
 1000 │ male        2  own
               985 rows omitted

julia> german[:, Cols("Age", Between("Sex", "Job"))]
1000×3 DataFrame
  Row │ Age    Sex     Job
      │ Int64  String  Int64
──────┼──────────────────────
    1 │     1  male        2
    2 │     1  female      2
    3 │    49  male        1
    4 │    45  male        2
    5 │    53  male        2
    6 │    35  male        1
    7 │    53  male        2
    8 │    35  male        3
  ⋮   │   ⋮      ⋮       ⋮
  994 │    30  male        3
  995 │    50  male        2
  996 │    31  female      1
  997 │    40  male        3
  998 │    38  male        2
  999 │    23  male        2
 1000 │    27  male        2
             985 rows omitted

julia> german[:, Cols("Age", Not("Sex"))]
1000×9 DataFrame
  Row │ Age    id     Job    Housing  Saving accounts  Checking account  Credi ⋯
      │ Int64  Int64  Int64  String   String           String            Int64 ⋯
──────┼─────────────────────────────────────────────────────────────────────────
    1 │     1      1      2  own      NA               little                  ⋯
    2 │     1      1      2  own      little           moderate
    3 │    49      2      1  own      little           NA
    4 │    45      3      2  free     little           little
    5 │    53      4      2  free     little           little                  ⋯
    6 │    35      5      1  free     NA               NA
    7 │    53      6      2  own      quite rich       NA
    8 │    35      7      3  rent     little           moderate
  ⋮   │   ⋮      ⋮      ⋮       ⋮            ⋮                ⋮                ⋱
  994 │    30    993      3  own      little           little                  ⋯
  995 │    50    994      2  own      NA               NA
  996 │    31    995      1  own      little           NA
  997 │    40    996      3  own      little           little
  998 │    38    997      2  own      little           NA                      ⋯
  999 │    23    998      2  free     little           little
 1000 │    27    999      2  own      moderate         moderate
                                                  3 columns and 985 rows omitted

julia> findall(==(67), german[!, :Age]) # It will find 67 in column `Age` of `german` data frame
2-element Vector{Int64}:
 555
 780


julia> findall(!=(67), german[:, :Age]) # It is showing where 67 is not occuring
998-element Vector{Int64}:
    1
    2
    3
    4
    5
    6
    7
    8
    9
   10
    ⋮
  992
  993
  994
  995
  996
  997
  998
  999
 1000
```

In the above example `german[:, Cols("Age", Not("Sex"))]`, `Age` column will come first after 
removing `sex` column. For more understanding if we will follow this example `german[:, Cols("Job", Not("Sex"))]`
then `Job` column will be placed first and then the remaining order will be same after removing `Sex` column.

You can also use `Regex` to select columns and `Not` from [InvertedIndices.jl](https://github.com/mbauman/InvertedIndices.jl) 
both to select rows and columns:
```jldoctest dataframe
julia> german[Not(5), r"Sex"]
999×1 DataFrame
 Row │ Sex
     │ String
─────┼────────
   1 │ male
   2 │ female
   3 │ male
   4 │ male
   5 │ male
   6 │ male
   7 │ male
   8 │ male
  ⋮  │   ⋮
 993 │ male
 994 │ male
 995 │ female
 996 │ male
 997 │ male
 998 │ male
 999 │ male
984 rows omitted
```

In the expression `german[!, Not(:Age)]` the `!` indicates that underlying columns are not copied:
```jldoctest dataframe
julia> german[!, Not(3)]
1000×9 DataFrame
  Row │ id     Age    Job    Housing  Saving accounts  Checking account  Credi ⋯
      │ Int64  Int64  Int64  String   String           String            Int64 ⋯
──────┼─────────────────────────────────────────────────────────────────────────
    1 │     1      1      2  own      NA               little                  ⋯
    2 │     1      1      2  own      little           moderate
    3 │     2     49      1  own      little           NA
    4 │     3     45      2  free     little           little
    5 │     4     53      2  free     little           little                  ⋯
    6 │     5     35      1  free     NA               NA
    7 │     6     53      2  own      quite rich       NA
    8 │     7     35      3  rent     little           moderate
  ⋮   │   ⋮      ⋮      ⋮       ⋮            ⋮                ⋮                ⋱
  994 │   993     30      3  own      little           little                  ⋯
  995 │   994     50      2  own      NA               NA
  996 │   995     31      1  own      little           NA
  997 │   996     40      3  own      little           little
  998 │   997     38      2  own      little           NA                      ⋯
  999 │   998     23      2  free     little           little
 1000 │   999     27      2  own      moderate         moderate
                                                  3 columns and 985 rows omitted
```

In the given code of block `:` means that the columns will get copied:
```jldoctest dataframe
julia> german[:, Not(2)]
1000×9 DataFrame
  Row │ id     Sex     Job    Housing  Saving accounts  Checking account  Cred ⋯
      │ Int64  String  Int64  String   String           String            Int6 ⋯
──────┼─────────────────────────────────────────────────────────────────────────
    1 │     1  male        2  own      NA               little                 ⋯
    2 │     1  female      2  own      little           moderate
    3 │     2  male        1  own      little           NA
    4 │     3  male        2  free     little           little
    5 │     4  male        2  free     little           little                 ⋯
    6 │     5  male        1  free     NA               NA
    7 │     6  male        2  own      quite rich       NA
    8 │     7  male        3  rent     little           moderate
  ⋮   │   ⋮      ⋮       ⋮       ⋮            ⋮                ⋮               ⋱
  994 │   993  male        3  own      little           little                 ⋯
  995 │   994  male        2  own      NA               NA
  996 │   995  female      1  own      little           NA
  997 │   996  male        3  own      little           little
  998 │   997  male        2  own      little           NA                     ⋯
  999 │   998  male        2  free     little           little
 1000 │   999  male        2  own      moderate         moderate
                                                  3 columns and 985 rows omitted
```

## Views

You can simply create a `view` of a data frame (it is more efficient than creating a materialized selection).
Here are the possible return value options.

```jldoctest dataframe
julia> german = copy(german_ref);

julia> s_german1 = view(german, :, 2:5) # column subsetting
1000×4 SubDataFrame
  Row │ Age    Sex     Job    Housing
      │ Int64  String  Int64  String
──────┼───────────────────────────────
    1 │    67  male        2  own
    2 │    22  female      2  own
    3 │    49  male        1  own
    4 │    45  male        2  free
    5 │    53  male        2  free
    6 │    35  male        1  free
    7 │    53  male        2  own
    8 │    35  male        3  rent
  ⋮   │   ⋮      ⋮       ⋮       ⋮
  994 │    30  male        3  own
  995 │    50  male        2  own
  996 │    31  female      1  own
  997 │    40  male        3  own
  998 │    38  male        2  own
  999 │    23  male        2  free
 1000 │    27  male        2  own
                      985 rows omitted

julia> s_german2 = @view german[end:-1:1, [1, 4]] # row and column subsetting
1000×2 SubDataFrame
  Row │ id     Job
      │ Int64  Int64
──────┼──────────────
    1 │   999      2
    2 │   998      2
    3 │   997      2
    4 │   996      3
    5 │   995      1
    6 │   994      2
    7 │   993      3
    8 │   992      1
  ⋮   │   ⋮      ⋮
  994 │     6      2
  995 │     5      1
  996 │     4      2
  997 │     3      2
  998 │     2      1
  999 │     1      2
 1000 │     0      2
     985 rows omitted
```
In the above example the number `2` and `5` represents the indices and as we know in `julia` indices start from `1`.
So, from `2:5` it will access the column from `Age` to `Housing`.

```jldoctest dataframe
julia> @view german[1:5, 1]
5-element view(::Vector{Int64}, 1:5) with eltype Int64:
 0
 1
 2
 3
 4

julia> @view german[2, 2]
0-dimensional view(::Vector{Int64}, 2) with eltype Int64:
22

julia> @view german[3, 2:5] # a DataFrameRow, the same as for german[3, 2:5] without a view
DataFrameRow
 Row │ Age    Sex     Job    Housing
     │ Int64  String  Int64  String
─────┼───────────────────────────────
   3 │    49  male        1  own

julia> @view german[2:5, 2:5] # here you can see creation of view is very fast
4×4 SubDataFrame
 Row │ Age    Sex     Job    Housing
     │ Int64  String  Int64  String
─────┼───────────────────────────────
   1 │    22  female      2  own
   2 │    49  male        1  own
   3 │    45  male        2  free
   4 │    53  male        2  free
```

In-place functions are safe to call, except when a view of the `DataFrame` (created via a `view`, `@view` or `*groupby*`) 
or when a `DataFrame` created with `copycols=false` are in use.

## Using `select`, and `select!`, `transform`, and `transform!`, `sort!`

You can also use the `select` and `select!` functions to select, rename, and transform columns in a data frame.

The `select` function creates a new data frame:

```jldoctest dataframe
julia> german = copy(german_ref);

julia> select(german, Not(:Age)) # drop column :Age in a new data frame
1000×9 DataFrame
  Row │ id     Sex     Job    Housing  Saving accounts  Checking account  Cred ⋯
      │ Int64  String  Int64  String   String           String            Int6 ⋯
──────┼─────────────────────────────────────────────────────────────────────────
    1 │     0  male        2  own      NA               little                 ⋯
    2 │     1  female      2  own      little           moderate
    3 │     2  male        1  own      little           NA
    4 │     3  male        2  free     little           little
    5 │     4  male        2  free     little           little                 ⋯
    6 │     5  male        1  free     NA               NA
    7 │     6  male        2  own      quite rich       NA
    8 │     7  male        3  rent     little           moderate
  ⋮   │   ⋮      ⋮       ⋮       ⋮            ⋮                ⋮               ⋱
  994 │   993  male        3  own      little           little                 ⋯
  995 │   994  male        2  own      NA               NA
  996 │   995  female      1  own      little           NA
  997 │   996  male        3  own      little           little
  998 │   997  male        2  own      little           NA                     ⋯
  999 │   998  male        2  free     little           little
 1000 │   999  male        2  own      moderate         moderate
                                                  3 columns and 985 rows omitted

julia> select(german, r"Sex") # select columns containing 'Sex' character
1000×1 DataFrame
  Row │ Sex
      │ String
──────┼────────
    1 │ male
    2 │ female
    3 │ male
    4 │ male
    5 │ male
    6 │ male
    7 │ male
    8 │ male
  ⋮   │   ⋮
  994 │ male
  995 │ male
  996 │ female
  997 │ male
  998 │ male
  999 │ male
 1000 │ male
985 rows omitted

julia> select(german, r"Sex","Job") # Selects more than one column
1000×2 DataFrame
  Row │ Sex     Job
      │ String  Int64
──────┼───────────────
    1 │ male        2
    2 │ female      2
    3 │ male        1
    4 │ male        2
    5 │ male        2
    6 │ male        1
    7 │ male        2
    8 │ male        3
  ⋮   │   ⋮       ⋮
  994 │ male        3
  995 │ male        2
  996 │ female      1
  997 │ male        3
  998 │ male        2
  999 │ male        2
 1000 │ male        2
      985 rows omitted

julia> select(german, :Sex => :x1, :Age => :x2) # rename columns
1000×2 DataFrame
  Row │ x1      x2
      │ String  Int64
──────┼───────────────
    1 │ male       67
    2 │ female     22
    3 │ male       49
    4 │ male       45
    5 │ male       53
    6 │ male       35
    7 │ male       53
    8 │ male       35
  ⋮   │   ⋮       ⋮
  994 │ male       30
  995 │ male       50
  996 │ female     31
  997 │ male       40
  998 │ male       38
  999 │ male       23
 1000 │ male       27
      985 rows omitted

julia> select(german, :Age, :Age => ByRow(sqrt)) # transform columns by row
1000×2 DataFrame
  Row │ Age    Age_sqrt
      │ Int64  Float64
──────┼─────────────────
    1 │    67   8.18535
    2 │    22   4.69042
    3 │    49   7.0
    4 │    45   6.7082
    5 │    53   7.28011
    6 │    35   5.91608
    7 │    53   7.28011
    8 │    35   5.91608
  ⋮   │   ⋮       ⋮
  994 │    30   5.47723
  995 │    50   7.07107
  996 │    31   5.56776
  997 │    40   6.32456
  998 │    38   6.16441
  999 │    23   4.79583
 1000 │    27   5.19615
        985 rows omitted
```

It is important to note that `select` always returns a data frame, even if a single column selected 
(as opposed to indexing syntax).

```jldoctest dataframe
julia> select(german, :Age)
1000×1 DataFrame
  Row │ Age
      │ Int64
──────┼───────
    1 │    67
    2 │    22
    3 │    49
    4 │    45
    5 │    53
    6 │    35
    7 │    53
    8 │    35
  ⋮   │   ⋮
  994 │    30
  995 │    50
  996 │    31
  997 │    40
  998 │    38
  999 │    23
 1000 │    27
985 rows omitted

julia> german[:, :Age]
1000-element Vector{Int64}:
 67
 22
 49
 45
 53
 35
 53
 35
 61
 28
  ⋮
 34
 23
 30
 50
 31
 40
 38
 23
 27
```

By default `select` copies columns of a passed source DataFrame. In order to avoid copying, pass `copycols=false`:

```jldoctest dataframe
julia> df = select(german, :Sex)
1000×1 DataFrame
  Row │ Sex
      │ String
──────┼────────
    1 │ male
    2 │ female
    3 │ male
    4 │ male
    5 │ male
    6 │ male
    7 │ male
    8 │ male
  ⋮   │   ⋮
  994 │ male
  995 │ male
  996 │ female
  997 │ male
  998 │ male
  999 │ male
 1000 │ male
985 rows omitted

julia> df.Sex === german.Sex
false

julia> df = select(german, :Sex, copycols=false)
1000×1 DataFrame
  Row │ Sex
      │ String
──────┼────────
    1 │ male
    2 │ female
    3 │ male
    4 │ male
    5 │ male
    6 │ male
    7 │ male
    8 │ male
  ⋮   │   ⋮
  994 │ male
  995 │ male
  996 │ female
  997 │ male
  998 │ male
  999 │ male
 1000 │ male
985 rows omitted

julia> df.Sex === german.Sex
true
```

To perform the selection operation in-place use `select!`:

```jldoctest dataframe
julia> german = copy(german_ref);

julia> select!(german, Not(:Age));

julia> german
1000×9 DataFrame
  Row │ id     Sex     Job    Housing  Saving accounts  Checking account  Cred ⋯
      │ Int64  String  Int64  String   String           String            Int6 ⋯
──────┼─────────────────────────────────────────────────────────────────────────
    1 │     0  male        2  own      NA               little                 ⋯
    2 │     1  female      2  own      little           moderate
    3 │     2  male        1  own      little           NA
    4 │     3  male        2  free     little           little
    5 │     4  male        2  free     little           little                 ⋯
    6 │     5  male        1  free     NA               NA
    7 │     6  male        2  own      quite rich       NA
    8 │     7  male        3  rent     little           moderate
  ⋮   │   ⋮      ⋮       ⋮       ⋮            ⋮                ⋮               ⋱
  994 │   993  male        3  own      little           little                 ⋯
  995 │   994  male        2  own      NA               NA
  996 │   995  female      1  own      little           NA
  997 │   996  male        3  own      little           little
  998 │   997  male        2  own      little           NA                     ⋯
  999 │   998  male        2  free     little           little
 1000 │   999  male        2  own      moderate         moderate
                                                  3 columns and 985 rows omitted
```

`transform` and `transform!` functions work identically to `select` and `select!` with the only difference that they retain all columns that are present in the source data frame.

In-place functions, whose names end with `!`, may mutate the data frame they take as an argument, for example:

```jldoctest dataframe
julia> german = copy(german_ref);

julia> sort!(german, :Age)
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

julia> german.Age[1] = 100 #here we assigned the value of 1st row is 100 in `Age` column
100

julia> german
1000×10 DataFrame
  Row │ id     Age    Sex     Job    Housing  Saving accounts  Checking accoun ⋯
      │ Int64  Int64  String  Int64  String   String           String          ⋯
──────┼─────────────────────────────────────────────────────────────────────────
    1 │   391    100  female      1  rent     rich             moderate        ⋯
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

julia> german.Age
1000-element Vector{Int64}:
 100
  19
  20
  20
  20
  20
  20
  20
  20
  20
   ⋮
  68
  68
  70
  74
  74
  74
  74
  75
  75
```
Note, that in the above example the original `Age` vector is mutated in the process.