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
```jldoctest
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

 !!! note

    DataFrames.jl allows to use `Symbol`s (like `:id`) and strings (like `"id"`)
    for all column indexing operations for convenience. However, using `Symbol`s
    is slightly faster and should generally be preferred, if not generating them
    via string manipulation.

## Examining the Data

You can adjust printing options by calling the `show` function manually: `show(german, allrows=true)`
prints all rows even if they do not fit on screen and `show(german, allcols=true)` does the same for
columns.

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

