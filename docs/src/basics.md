# Starting with DataFrames

The first and imporatant part to work with DataFrames.jl, is to install the `DataFrames.jl` package which is available thorugh the Julia package system and can be installed using the following commands:

```julia
using Pkg
Pkg.add("DataFrames")
```

Or,

```julia
julia> ]

julia> add DataFrames
```

To make sure everything works as expected, try to load the package and if you have the time execute its test suits:
```julia
using DataFrames
using Pkg
Pkg.test("DataFrames")
```

Throughout the rest of the tutorial we will assume that you have installed the DataFrames package and have already typed `using DataFrames` to bring all of the relevant variables into your current namespace.

# The DataFrame Type

The object of the `DataFrame` type represent a data table as a series of vectors, each correspondng to a column or variable. The simplest way of constructing a `DataFrame` is to pass your `CSV` file `using CSV` and the operation will look like:

```jldoctest
julia> using DataFrames

julia> using CSV

julia> german = CSV.read((joinpath(dirname(pathof(DataFrames)),
                                 "..", "docs", "src", "assets", "german.csv")),
                       DataFrame) 
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
    9 │     8     61  male        1  own      rich             NA                         3059        12  radio/TV
   10 │     9     28  male        3  own      little           moderate                   5234        30  car
   11 │    10     25  female      2  rent     little           moderate                   1295        12  car
   12 │    11     24  female      2  rent     little           little                     4308        48  business
   13 │    12     22  female      2  own      little           moderate                   1567        12  radio/TV
   14 │    13     60  male        1  own      little           little                     1199        24  car
   15 │    14     28  female      2  rent     little           little                     1403        15  car
   16 │    15     32  female      1  own      moderate         little                     1282        24  radio/TV
   17 │    16     53  male        2  own      NA               NA                         2424        24  radio/TV
   18 │    17     25  male        2  own      NA               little                     8072        30  business
  ⋮   │   ⋮      ⋮      ⋮       ⋮       ⋮            ⋮                ⋮                ⋮           ⋮               ⋮
  984 │   983     26  male        2  own      little           little                     8229        36  car
  985 │   984     30  male        1  own      little           NA                         2028        24  furniture/equipment
  986 │   985     25  female      2  rent     little           little                     1433        15  furniture/equipment
  987 │   986     33  male        2  own      little           rich                       6289        42  business
  988 │   987     64  female      2  own      moderate         NA                         1409        13  radio/TV
  989 │   988     29  male        3  free     little           little                     6579        24  car
  990 │   989     48  male        1  own      little           moderate                   1743        24  radio/TV
  991 │   990     37  male        1  own      NA               NA                         3565        12  education
  992 │   991     34  male        1  own      moderate         NA                         1569        15  radio/TV
  993 │   992     23  male        1  rent     NA               little                     1936        18  radio/TV
  994 │   993     30  male        3  own      little           little                     3959        36  furniture/equipment
  995 │   994     50  male        2  own      NA               NA                         2390        12  car
  996 │   995     31  female      1  own      little           NA                         1736        12  furniture/equipment
  997 │   996     40  male        3  own      little           little                     3857        30  car
  998 │   997     38  male        2  own      little           NA                          804        12  radio/TV
  999 │   998     23  male        2  free     little           little                     1845        45  radio/TV
 1000 │   999     27  male        2  own      moderate         moderate                   4576        45  car
                                                                                                              965 rows omitted
```

To access the columns directly (i.e. without copying) you can use `german.col`, `german."col"`, `german[!, :col]` or `german[!, "col"]`. The two latter syntaxes are more flexible as they allow us passing a variable holding the name of the column, and not only a literal name. Columns name can be either symbols (written as `:col`, `:var"col"` or `Symbol("col")`) or strings (written as `"col"`). Variable interpolation into the string using `$` does not work if you have forms like `german."col"` and `:var"col"`. You can also access the column using an integer index specifying their position. 

Since `german[!, :col]` does not make a copy, changing the elements of the column vector returned by this sysntax will affect the values stored in the original `german`. To get a **copy** of the column you can use `german[:, :col]`: changing the vector returned by this syntax does not change `german`.

```jldoctest
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
 "female"
 "female"
 "female"
 "male"
 "female"
 "female"
 "male"
 "male"
 "female"
 "male"
 ⋮
 "male"
 "female"
 "male"
 "male"
 "female"
 "male"
 "female"
 "male"
 "male"
 "male"
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
```jldoctest
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
 ```jldoctest
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

# Examining the Data

You can adjust printing options by calling the `show` function manually: `show(german, allrows=true)` prints all rows even if they do not fit on screen and `show(german, allcols=true)` does the same for columns.

If you want to look at `first` and `last` rows of a dataframe (respectively) then you can do this using `first` and `last` functions:

```jldoctest
julia> first(german, 6)
8×10 DataFrame
 Row │ id     Age    Sex     Job    Housing  Saving accounts  Checking account  Credit amount  Duration  Purpose
     │ Int64  Int64  String  Int64  String   String           String            Int64          Int64     String
─────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
   1 │     0     67  male        2  own      NA               little                     1169         6  radio/TV
   2 │     1     22  female      2  own      little           moderate                   5951        48  radio/TV
   3 │     2     49  male        1  own      little           NA                         2096        12  education
   4 │     3     45  male        2  free     little           little                     7882        42  furniture/equipment
   5 │     4     53  male        2  free     little           little                     4870        24  car
   6 │     5     35  male        1  free     NA               NA                         9055        36  education
   7 │     6     53  male        2  own      quite rich       NA                         2835        24  furniture/equipment
   8 │     7     35  male        3  rent     little           moderate                   6948        36  car

julia> last(german, 5)
5×10 DataFrame
 Row │ id     Age    Sex     Job    Housing  Saving accounts  Checking account  Credit amount  Duration  Purpose
     │ Int64  Int64  String  Int64  String   String           String            Int64          Int64     String
─────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
   1 │   995     31  female      1  own      little           NA                         1736        12  furniture/equipment
   2 │   996     40  male        3  own      little           little                     3857        30  car
   3 │   997     38  male        2  own      little           NA                          804        12  radio/TV
   4 │   998     23  male        2  free     little           little                     1845        45  radio/TV
   5 │   999     27  male        2  own      moderate         moderate                   4576        45  car
```

# Taking a Subset

## Indexing Syntax

Specific subsets of a dataframe can be extracted using the indexing syntax, similar to matrices. In the [Indexing](https://dataframes.juliadata.org/stable/lib/indexing/#Indexing) section of the manual you can find all details about the available options. Here we highlight the basic options.

The colon `:` indicates that all items (rows or columns depending on its position) should be retained:

```jldoctest
julia> german[1:5, :]
5×10 DataFrame
 Row │ id     Age    Sex     Job    Housing  Saving accounts  Checking account  Credit amount  Duration  Purpose
     │ Int64  Int64  String  Int64  String   String           String            Int64          Int64     String
─────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
   1 │     0     67  male        2  own      NA               little                     1169         6  radio/TV
   2 │     1     22  female      2  own      little           moderate                   5951        48  radio/TV
   3 │     2     49  male        1  own      little           NA                         2096        12  education
   4 │     3     45  male        2  free     little           little                     7882        42  furniture/equipment
   5 │     4     53  male        2  free     little           little                     4870        24  car

julia> german[[1, 6, 15], :]
3×10 DataFrame
 Row │ id     Age    Sex     Job    Housing  Saving accounts  Checking account  Credit amount  Duration  Purpose
     │ Int64  Int64  String  Int64  String   String           String            Int64          Int64     String
─────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────
   1 │     0     67  male        2  own      NA               little                     1169         6  radio/TV
   2 │     5     35  male        1  free     NA               NA                         9055        36  education
   3 │    14     28  female      2  rent     little           little                     1403        15  car

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
    9 │    61  male
   10 │    28  male
   11 │    25  female
   12 │    24  female
   13 │    22  female
   14 │    60  male
   15 │    28  female
   16 │    32  female
   17 │    53  male
   18 │    25  male
  ⋮   │   ⋮      ⋮
  984 │    26  male
  985 │    30  male
  986 │    25  female
  987 │    33  male
  988 │    64  female
  989 │    29  male
  990 │    48  male
  991 │    37  male
  992 │    34  male
  993 │    23  male
  994 │    30  male
  995 │    50  male
  996 │    31  female
  997 │    40  male
  998 │    38  male
  999 │    23  male
 1000 │    27  male
      965 rows omitted

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

Pay attention that `german[!, [:Sex]]` and `german[:, [:Sex]]` return a `DataFrame` object, while `german[!, :Sex]` and `german[:, :Sex]` return a vector:

```jldoctest
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
    9 │ male
   10 │ male
   11 │ female
   12 │ female
   13 │ female
   14 │ male
   15 │ female
   16 │ female
   17 │ male
   18 │ male
  ⋮   │   ⋮
  984 │ male
  985 │ male
  986 │ female
  987 │ male
  988 │ female
  989 │ male
  990 │ male
  991 │ male
  992 │ male
  993 │ male
  994 │ male
  995 │ male
  996 │ female
  997 │ male
  998 │ male
  999 │ male
 1000 │ male
965 rows omitted

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
 "female"
 "female"
 "female"
 "male"
 "female"
 "female"
 "male"
 "male"
 "female"
 "male"
 ⋮
 "male"
 "female"
 "male"
 "male"
 "female"
 "male"
 "female"
 "male"
 "male"
 "male"
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

In the first case, `[:Sex]` is a vector, indicating that the resulting object should be a `DataFrame`. On the other hand, `:Sex` is a single symbol, indicating that a single column vector should be extracted. Note that in the first case a vector is required to be passed (not just any iterable), so e.g. `german[:, (:Age, :Sex)]` is not allowed, but `german[:, [:Age, :Sex]]` is valid. 

A `Not` selector (from the [InvertedIndices](https://github.com/mbauman/InvertedIndices.jl) package) can be used to select all columns excluding a specific subset:

```jldoctest
julia> german[!, Not(:Age)]
1000×9 DataFrame
  Row │ id     Sex     Job    Housing  Saving accounts  Checking account  Credit amount  Duration  Purpose
      │ Int64  String  Int64  String   String           String            Int64          Int64     String
──────┼────────────────────────────────────────────────────────────────────────────────────────────────────────────────
    1 │     0  male        2  own      NA               little                     1169         6  radio/TV
    2 │     1  female      2  own      little           moderate                   5951        48  radio/TV
    3 │     2  male        1  own      little           NA                         2096        12  education
    4 │     3  male        2  free     little           little                     7882        42  furniture/equipment
    5 │     4  male        2  free     little           little                     4870        24  car
    6 │     5  male        1  free     NA               NA                         9055        36  education
    7 │     6  male        2  own      quite rich       NA                         2835        24  furniture/equipment
    8 │     7  male        3  rent     little           moderate                   6948        36  car
    9 │     8  male        1  own      rich             NA                         3059        12  radio/TV
   10 │     9  male        3  own      little           moderate                   5234        30  car
   11 │    10  female      2  rent     little           moderate                   1295        12  car
   12 │    11  female      2  rent     little           little                     4308        48  business
   13 │    12  female      2  own      little           moderate                   1567        12  radio/TV
   14 │    13  male        1  own      little           little                     1199        24  car
   15 │    14  female      2  rent     little           little                     1403        15  car
   16 │    15  female      1  own      moderate         little                     1282        24  radio/TV
   17 │    16  male        2  own      NA               NA                         2424        24  radio/TV
   18 │    17  male        2  own      NA               little                     8072        30  business
  ⋮   │   ⋮      ⋮       ⋮       ⋮            ⋮                ⋮                ⋮           ⋮               ⋮
  984 │   983  male        2  own      little           little                     8229        36  car
  985 │   984  male        1  own      little           NA                         2028        24  furniture/equipment
  986 │   985  female      2  rent     little           little                     1433        15  furniture/equipment
  987 │   986  male        2  own      little           rich                       6289        42  business
  988 │   987  female      2  own      moderate         NA                         1409        13  radio/TV
  989 │   988  male        3  free     little           little                     6579        24  car
  990 │   989  male        1  own      little           moderate                   1743        24  radio/TV
  991 │   990  male        1  own      NA               NA                         3565        12  education
  992 │   991  male        1  own      moderate         NA                         1569        15  radio/TV
  993 │   992  male        1  rent     NA               little                     1936        18  radio/TV
  994 │   993  male        3  own      little           little                     3959        36  furniture/equipment
  995 │   994  male        2  own      NA               NA                         2390        12  car
  996 │   995  female      1  own      little           NA                         1736        12  furniture/equipment
  997 │   996  male        3  own      little           little                     3857        30  car
  998 │   997  male        2  own      little           NA                          804        12  radio/TV
  999 │   998  male        2  free     little           little                     1845        45  radio/TV
 1000 │   999  male        2  own      moderate         moderate                   4576        45  car
                                                                                                       965 rows omitted
```

## Not, Between, Cols, and All selectors

Finally, you can use `Not`, `Between`, `Cols`, and `All` selectors in more complex column selection scenarioes (note that `Cols()` selects no columns while `All()` selects all columns therefore `Cols` is a preferred selector if you write generic code). The following examples move all columns whose names match `r"x"` regular expression respectively to the front and the end of a dataframe:

```jldoctest
julia> 