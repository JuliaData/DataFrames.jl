# Starting with DataFrames

The first and imporatant part to work with DataFrames.jl, is to install the `DataFrames.jl`
package which is available thorugh the Julia package system and can be installed using the 
following commands:

```julia
using Pkg
Pkg.add("DataFrames")
```

Or,

```julia
julia> ]

julia> add DataFrames
```

To make sure everything works as expected, try to load the package and if you have the time execute 
its test suits:
```julia
using DataFrames
using Pkg
Pkg.test("DataFrames")
```

Throughout the rest of the tutorial we will assume that you have installed the DataFrames package and 
have already typed `using DataFrames` to bring all of the relevant variables into your current namespace.
Let's get started by loading the DataFrames package:
```julia
using DataFrames
```

The object of the `DataFrame` type represent a data table as a series of vectors, each correspondng to a 
column or variable.

# Constructors and basic utility functions

## Constructors

In this section you will see many ways to create a DataFrame using `DataFrame()` constructor.
First, we could create an empty DataFrame:
```jldoctest dataframe
julia> using DataFrames
julia> df = DataFrame()
0×0 DataFrame
```

Or, we could call the constructor using keyword arguments to add columns to the DataFrame:
```jldoctest dataframe
julia> df = DataFrame(A=1:3, B=5:7, fixed=1)
3×3 DataFrame
 Row │ A      B      fixed
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      5      1
   2 │     2      6      1
   3 │     3      7      1
```
note in column `:fixed` that scalars get automatically broadcasted.

Now, we will explore how to load a CSV file into into a DataFrame. Unlike Python's Pandas `read_csv`
the functions in Julia are separated into two modules - `CSV.jl` and `DataFrames.jl`. As the first 
step, you have to declare the libraries you will use. In our case `CSV` and `DataFrames`. In order to
turn the `CSV.File` to a DataFrame you have to pass it to the `DataFrames.DataFrame` object. You can 
wrap DataFrame around the `CSV.read(path; kwargs)`.

```jldoctest dataframe
julia> using DataFrames

julia> using CSV

julia> german = CSV.read((joinpath(dirname(pathof(DataFrames)),
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
```

You can see that Julia representation (unlike python pandas) displays the data type of the column, 
whether it is a `string`, `int` or `float`.

To access the columns directly (i.e. without copying) you can use `german.col`, `german."col"`,
`german[!, :col]` or `german[!, "col"]`. The two latter syntaxes are more flexible as they allow 
us passing a variable holding the name of the column, and not only a literal name. Columns name 
can be either symbols (written as `:col`, `:var"col"` or `Symbol("col")`) or strings (written as `"col"`). 
Variable interpolation into the string using `$` does not work if you have forms like `german."col"` 
and `:var"col"`. You can also access the column using an integer index specifying their position. 

Since `german[!, :col]` does not make a copy, changing the elements of the column vector returned by this 
sysntax will affect the values stored in the original `german`. To get a **copy** of the column you can use
`german[:, :col]`: changing the vector returned by this syntax does not change `german`.

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

julia> german.Housing === german[:, 5]
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

 you can also get column names with a given `eltype`:
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

 To get the element types of columns use `elrype` on `eachcol(german)`
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

To remove all rows and columns from a DataFrame you can use `empty` and `empty!` functions to remove all rows from a DataFrame:
```jldoctest dataframe
julia> empty(german)
0×10 DataFrame

julia> german
1000×10 DataFrame
  Row │ id     Age    Sex     Job    Housing  Saving accounts  Checking accoun ⋯
      │ Int64  Int64  String  Int64  String   String           String          ⋯
──────┼─────────────────────────────────────────────────────────────────────────
    1 │     1      1  male        2  own      NA               little          ⋯
    2 │     1      1  female      2  own      little           moderate
    3 │     2      2  male        1  own      little           NA
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

## Getting basic information about a DataFrame

In this section we will learn about how to get basic information on our `German` DataFrame:

The standard `size` function works to get dimensions of the DataFrame,
```jldoctest dataframe
julia> julia> german = CSV.read((joinpath(dirname(pathof(DataFrames)),
                                 "..", "docs", "src", "assets", "german.csv")),
                       DataFrame) ;
```

```jldoctest dataframe
julia> size(german), size(german, 1), size(german, 2)
((1000, 10), 1000, 10)
```

To get the number of Rows or Columns in an AbstractDataFrame `german`, you can use `nrow()` and `ncol()`,
```jldoctest dataframe
julia> nrow(german), ncol(german)
(1000, 10)
```

To get basic summaer statistics of data in your DataFrame use `describe` (check out the help of describe for information on how to customize shown statistics).
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

You can adjust printing options by calling the `show` function manually: `show(german, allrows=true)`
prints all rows even if they do not fit on screen and `show(german, allcols=true)` does the same for
columns.

You can also compute descriptive statistics directly on indovidual columns:

```jldoctest dataframe
julia> using Statistics

julia>  mean(german.Age)
35.546
```

If you want to look at `first` and `last` rows of a dataframe (respectively) then you can do this 
using `first` and `last` functions:

```jldoctest dataframe
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
last(german, 5)
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

Using `first` and `last` without number of rows will return a first/last DataFrameRow in the DataFrame:
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

## Taking a Subset

### Indexing Syntax

Specific subsets of a dataframe can be extracted using the indexing syntax, similar to matrices. 
In the [Indexing](https://dataframes.juliadata.org/stable/lib/indexing/#Indexing) section of the 
manual you can find all details about the available options. Here we highlight the basic options.

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
german[1:5, [:Sex, :Age]]
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

Pay attention that `german[!, [:Sex]]` and `german[:, [:Sex]]` return a `DataFrame` object, 
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

In the first case, `[:Sex]` is a vector, indicating that the resulting object should be a `DataFrame`. 
On the other hand, `:Sex` is a single symbol, indicating that a single column vector should be extracted. 
Note that in the first case a vector is required to be passed (not just any iterable), 
so e.g. `german[:, (:Age, :Sex)]` is not allowed, but `german[:, [:Age, :Sex]]` is valid. 

### Most elementary `get` and `set` operations

Given the DataFrame `german` earlier we have created earlier, here are various ways to grab one of its columns as vector:
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

julia> german.Age, german[!, 2], german[!, :Age] # all get the vector stored in out DataFrame without copying it
([67, 22, 49, 45, 53, 35, 53, 35, 61, 28  …  37, 34, 23, 30, 50, 31, 40, 38, 23, 27], [67, 22, 49, 45, 53, 35, 53, 35, 61, 28  …  37, 34, 23, 30, 50, 31, 40, 38, 23, 27], [67, 22, 49, 45, 53, 35, 53, 35, 61, 28  …  37, 34, 23, 30, 50, 31, 40, 38, 23, 27])

julia> german."Sex", german[!, "Sex"] # the same using string indexing
(["male", "female", "male", "male", "male", "male", "male", "male", "male", "male"  …  "male", "male", "male", "male", "male", "female", "male", "male", "male", "male"], ["male", "female", "male", "male", "male", "male", "male", "male", "male", "male"  …  "male", "male", "male", "male", "male", "female", "male", "male", "male", "male"])

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

To grab one row as DataFrame, we can index as follows:
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

## Not, Between, Cols, and All selectors

Finally, you can use `Not`, `Between`, `Cols`, and `All` selectors in more complex column selection 
scenarioes (note that `Cols()` selects no columns while `All()` selects all columns therefore `Cols` 
is a preferred selector if you write generic code). The following examples move all columns whose 
names match `r"x"` regular expression respectively to the front and the end of a dataframe. Now we will
see how `cols` and `Between` can be used to select columns of a DataFrame.

A `Not` selector (from the [InvertedIndices](https://github.com/mbauman/InvertedIndices.jl) package) can be 
used to select all columns excluding a specific subset:

```jldoctest dataframe
julia> german[!, Not(:Age)]
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
    1 │    67  male        2
    2 │    22  female      2
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
    1 │    67      0      2  own      NA               little                  ⋯
    2 │    22      1      2  own      little           moderate
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
```

You can also use `Regex` to select columns and `Not` from `InvertedIndices.jl` both to select rows and columns:
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

`german[!, Not(Age)]` ! indicates that underlying columns are not copied:
```jldoctest dataframe
julia> german[!, Not(3)]
1000×9 DataFrame
  Row │ id     Age    Job    Housing  Saving accounts  Checking account  Credi ⋯
      │ Int64  Int64  Int64  String   String           String            Int64 ⋯
──────┼─────────────────────────────────────────────────────────────────────────
    1 │     0     67      2  own      NA               little                  ⋯
    2 │     1     22      2  own      little           moderate
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

Assignment of a scalar to a DataFrame can be done in ranges using broadcasting:
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
    3 │     2      2  male        1  own      little           NA
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
julia> german[in.(german.id, Ref([1, 6, 908, 955])), :]
4×10 DataFrame
 Row │ id     Age    Sex     Job    Housing  Saving accounts  Checking account ⋯
     │ Int64  Int64  String  Int64  String   String           String           ⋯
─────┼──────────────────────────────────────────────────────────────────────────
   1 │     1     22  female      2  own      little           moderate         ⋯
   2 │     6     53  male        2  own      quite rich       NA
   3 │   908     46  female      1  own      little           NA
   4 │   955     57  female      3  rent     rich             little
                                                               3 columns omitted
```

Equivalently, the `in` function can be called with a single argument to create a function object that 
tests whether each value belongs to the subset (partial application of `in`):
`german[in([1, 6, 908, 955]).(german.id), :]`

## Views

You can simply create a `view` of a DataFrame (it is more efficient than creating a materialized selection). 
Here are the possible return value options.

```jldoctest dataframe
julia> @view german[1:5, 1]
5-element view(::PooledArrays.PooledVector{String, UInt32, Vector{UInt32}}, 1:5) with eltype String:
 "male"
 "female"
 "male"
 "male"
 "male"

julia> @view german[2, 2]
0-dimensional view(::Vector{Int64}, 2) with eltype Int64:
22

julia> @view german[3, 2:5] # a DataFrameRow, the same as for german[3, 2:5] without a view
DataFrameRow
 Row │ Age    Sex     Job    Housing
     │ Int64  String  Int64  String
─────┼───────────────────────────────
   3 │    49  male        1  own

julia> @view german[2:5, 2:5] # a SubDataFrame
4×4 SubDataFrame
 Row │ Age    Sex     Job    Housing
     │ Int64  String  Int64  String
─────┼───────────────────────────────
   1 │    22  female      2  own
   2 │    49  male        1  own
   3 │    45  male        2  free
   4 │    53  male        2  free
```

## Using `select`, and `select!`, `transform`, and `transform!`, `sort!`

You can also use the `select` and `select!` functions to select, rename, and transform columns in a DataFrame.

The `select` function creates a new DataFrame:

```jldoctest dataframe
julia> german = CSV.read((joinpath(dirname(pathof(DataFrames)),
                                 "..", "docs", "src", "assets", "german.csv")),
                       DataFrame);

julia> select(german, Not(:Age)) # drop column :x1 in a new data frame
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

julia> select(german, r"Sex") # select columns containing 'x' character
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

It is always important to note that `select` always returns a DataFrame, even if a single column selected (as opposed to indexing syntax).

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

By default `select` copis columns of a passed source DataFrame. In order to avoid copying, pass `copycols=false`:

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
julia> german = CSV.read((joinpath(dirname(pathof(DataFrames)),
                                 "..", "docs", "src", "assets", "german.csv")),
                       DataFrame);

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

`transform` and `transform!` functions work identically to `select` and `select!` with the only difference that they retain all columns that are present in the source DataFrame.

On the other hand, in-place functions, whose names end with `!`, may mutate the column vectors of the `DataFrame` they take as an argument, for example:

```jldoctest dataframe
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

julia> 1000×10 DataFrame
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

julia> german.Age[1] = 100
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

Note, that in the above example the original `Age` vector is not mutated in the process. 

In-place functions are safe to call, except when a view of the `DataFrame` (created via a `view`, `@view` or `*groupby*`) or when a `DataFrame` created with `copycols=false` are in use.

It is possible to have a direct access to a column `col` of a `DataFrame` `german` using the syntaxes `german.col`, `german[!, :col]`, via the `*eachcol*` function, by accessing a parent of a view of a column of a `DataFrame`, or simply by storing the reference to the column vector before the `DataFrame` was created with `copycols=false`.


