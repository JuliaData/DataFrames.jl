# First Steps with DataFrames.jl

If want to use the DataFrames.jl package you need to install it first.
You can do it using the following commands:

```julia
julia> using Pkg
julia> Pkg.add("DataFrames")
```

or

```julia
julia> ] # `]` should be pressed

(@v1.6) pkg> add DataFrames
```

If you want to make sure everything works as expected you can run the tests bundled
with DataFrames.jl, but be warned that it will take more than 30 minutes.
```julia
julia> using Pkg
julia> Pkg.test("DataFrames")
```

Additionally, it is recommended to check the version installed with the `status` command.
```julia
julia> ]

(@v1.6) pkg> status DataFrames
      Status `C:\Users\TeAmp0is0N\.julia\environments\v1.6\Project.toml`
  [a93c6f00] DataFrames v1.1.1
```

Throughout the rest of the tutorial we will assume that you have installed the DataFrames.jl package and
have already typed `using DataFrames` which loads the package:
```jldoctest dataframe
julia> using DataFrames
```

The most fundamental type provided by DataFrames.jl is `DataFrame`, where typically
each row is interpreted as an observation and each column as a feature.

# Constructors and Basic Utility Functions

## Constructors

In this section you will see many ways to create a `DataFrame` using the constructor.
First, let's create an empty `DataFrame`:
```jldoctest dataframe
julia> DataFrame()
0×0 DataFrame
```

Or, we could call the constructor using keyword arguments to add columns to the `DataFrame`:
```jldoctest dataframe
julia> DataFrame(A=1:3, B=5:7, fixed=1)
3×3 DataFrame
 Row │ A      B      fixed
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      5      1
   2 │     2      6      1
   3 │     3      7      1
```
note that in column `:fixed` scalar `1` gets automatically broadcasted.

To move forward with the tutorial you need to install the CSV.jl package in your environment.
In order to do so run the following commands:
```julia
julia> using Pkg
julia> Pkg.add("CSV")
```
Make sure you have CSV.jl in a version that is at least 1.0.

Before moving forward, we would like to discuss `Cropping` which you will found in many examples
that some of the outputs are cropped. The keyword `crop` can be used to define how the output will
be cropped if the display has limits. The default behavious depends on the property `:limit` of the 
`io`. If `io` has `:limit => true`, the default value of `crop` is `:both`. Otherwise, if `:limit => false`
or it is not defined at all, then `crop` defaults to `:none`. To know more about cropping use this 
[link](https://ronisbr.github.io/PrettyTables.jl/stable/man/text_backend/#Cropping).

Now, we will explore how to load a CSV file into a `DataFrame`. Unlike Python's Pandas `read_csv`
you need two packages to accomplish this: CSV.jl and DataFrames.jl. As the first
step, you have to load the libraries you will use. In our case CSV.jl and DataFrames.jl. In order to
read the file in we will use the `CSV.read` function.
```jldoctest dataframe
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

julia> german = copy(german_ref); # It will copy the data frame
```

Now let's talk about the given code block:
```julia
german_ref = CSV.read(joinpath(dirname(pathof(DataFrames)),
                               "..", "docs", "src", "assets", "german.csv"),
                      DataFrame)
```
- we are storing `german.csv` file in the DataFrames.jl repository to make user's life easier and
  avoid having to download it each time;
- `pathof(DataFrames)` gives us the full path of the file that was used to import the DataFrames.jl package;
- first we split the directory part from it using `dirname`;
- then from this directory we need to move to the directory where `german.csv` is stored; we use
  `joinpath` as this is a recommended way to compose paths to resources stored on disk in an operating
  system independent way (remember that Widnows and Unix differ as they use either `/` or `\` as path
  separator; the `joinpath` function ensures we are not running into issues with this);
- then we read the CSV file; the second argument to `CSV.read` is `DataFrame` to indicate that we want to
  read in the file into a `DataFrame` (as `CSV.read` allows for many different target formats of data it
  can read-into).

You can see that DataFrames.jl (unlike Python's Pandas) displays the data type of the column,
In our case, it is an `Int64`, or `String`.

To access the columns directly (i.e. without copying) you can use `german.Sex`, `german."Sex"`,
`german[!, :Sex]` or `german[!, "Sex"]`. The two latter syntaxes are more flexible as they allow
us passing a variable holding the name of the column, and not only a literal name.

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
syntax will affect the values stored in the original `german` data frame. To get a **copy** of the column you can use
`german[:, :Sex]`: changing the vector returned by this syntax does not affect the `german` data frame.

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

julia> german.Sex === german[!, :Sex] # The `===` function confirms that both expressions produce the same object. 
true

julia> german.Sex === german[:, :Sex]
false
```

You can obtain the column names of the data frame as `String`s using the `names` function:
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

By broadcasting the `eltype` function over `eachcol(german)` iterator of columns stored
in the data frame we can get element types of columns:
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

To remove all rows from a `DataFrame` you can use `empty` and `empty!` functions:
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

In the above example `empty` function created a new `DataFrame` with the same column names
and column element types as `german` but with zero rows. On the other hand `empty!` function
removed all rows from `german` in-place and made each of its column empty.
The difference between the behavior of the `empty` and `empty!` functions is an application of the
[stylistic convention](https://docs.julialang.org/en/v1/manual/variables/#Stylistic-Conventions)
employed in the Julia language.
This convention is followed in all functions provided by the DataFrames.jl package.

## Getting Basic Information about a Data Frame

In this section we will learn about how to get basic information on our `german` `DataFrame`:

The `size` function returns the dimensions of the data frame.
First we restore the `german` data frame, as we have just emptied it above.
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
the column. `missing` values are skipped when computing the summary statistics.

You can adjust how data frame is displayed by calling the `show` function manually: `show(german, allrows=true)`
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

the `mapcols` function returns a `DataFrame` where each column of the source data frame is transformed using a passed function.
Note that `mapcols` guarantees not to reuse the columns from `german` in the returned `DataFrame`. If the transformation returns
its argument then it gets copied before being stored.

```jldoctest dataframe
julia> mapcols(id -> id .^2, german)
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
using the `first` and `last` functions:

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
`DataFrame` and information about which row and columns from the parent are selected.
You can think of `DataFrameRow` as a `NamedTuple` that is mutable,
i.e. allows to update the source data frame, which is often useful.

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

# Taking a Subset of a Data Frame

## Indexing Syntax

Specific subsets of a data frame can be extracted using the indexing syntax, similar to matrices.
In the [Indexing](https://dataframes.juliadata.org/stable/lib/indexing/#Indexing) section of the
manual you can find all details about the available options. Here we highlight the basic ones.

The general syntax of indexing is `data_frame[selected_rows, selected_columns]`. Observe that, as 
opposed to matrices in Julia Base, it is required to pass both row and column selector. The colon `:` 
indicates that all items (rows or columns depending on its position) should be retained:

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
```

In the example below we created a `DataFrame` having `:Sex` and `:Age` columns
and the first five rows of the `german` data set:

```jldoctest dataframe
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
while `german[!, :Sex]` and `german[:, :Sex]` return a vector. In the first case, `[:Sex]` 
is a vector, indicating that the resulting object should be a data frame. On the other hand,
`:Sex` is a single symbol, indicating that a single column vector should be extracted. Note 
that in the first case a vector is required to be passed (not just any iterable), so e.g. 
`german[:, (:Age, :Sex)]` is not allowed, but `german[:, [:Age, :Sex]]` is valid.

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

As it was explained above the difference between using `!` and `:`
when passing a row index is that `!` does not perform a copy of columns, while `:` does.
Therefore `german[!, [:Sex]]` data frame stores the same vector as the source `german` data frame,
while `german[:, [:Sex]]` stores its copy. The `!` selector normally should be avoided
as using it can lead to hard to catch bugs. However, when working with very large data frames
it can be useful to save memory and improve performance of operations.


Recapping what we have already learned,
given the data frame `german` here are various ways to grab one of its columns as vector:
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

julia> german.Age # get the vector stored in our DataFrame without copying it
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

julia> german."Sex" # the same using string
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

To get two columns as a `DataFrame`, we can index as follows:
```jldoctest dataframe
julia> german[:, 1:2]
1000×2 DataFrame
  Row │ id     Age
      │ Int64  Int64
──────┼──────────────
    1 │     0     67
    2 │     1     22
    3 │     2     49
    4 │     3     45
    5 │     4     53
    6 │     5     35
    7 │     6     53
    8 │     7     35
  ⋮   │   ⋮      ⋮
  994 │   993     30
  995 │   994     50
  996 │   995     31
  997 │   996     40
  998 │   997     38
  999 │   998     23
 1000 │   999     27
     985 rows omitted

julia> german[:, [3]] # here we have created a single column data frame
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
```

Compare this with accessing the vector contained in column `3`:

```jldoctest dataframe
julia> german[:, 3]
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

We can get a single cell of a data frame with the same syntax to get a cell of a matrix:
```jldoctest dataframe
julia> german[4, 4]
2
```

To get a new `DataFrame` that is subset of rows and columns do e.g.:

```jldoctest dataframe
julia> german[4:5, 4:5]
2×2 DataFrame
 Row │ Job    Housing
     │ Int64  String
─────┼────────────────
   1 │     2  free
   2 │     2  free
```

## Views

We can create a `view` of a data frame (it is more memory efficient than creating 
a materialized selection). Here are the possible return value options.

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

In the above example we used both a function call and a macro.

```jldoctest dataframe
julia> @view german[1:5, 1] # view the element type in vector form
5-element view(::Vector{Int64}, 1:5) with eltype Int64:
 0
 1
 2
 3
 4

julia> @view german[2, 2] # view of the number at 2nd row and 2nd column
0-dimensional view(::Vector{Int64}, 2) with eltype Int64:
22

julia> @view german[3, 2:5] # a DataFrameRow, the same as for german[3, 2:5] without a view
DataFrameRow
 Row │ Age    Sex     Job    Housing
     │ Int64  String  Int64  String
─────┼───────────────────────────────
   3 │    49  male        1  own
```

```julia
julia> @time german[2:5, 2:5]
  0.000079 seconds (21 allocations: 1.531 KiB)
4×4 DataFrame
 Row │ Age    Sex     Job    Housing
     │ Int64  String  Int64  String
─────┼───────────────────────────────
   1 │    22  female      2  own
   2 │    49  male        1  own
   3 │    45  male        2  free
   4 │    53  male        2  free

julia> @view german[2:5, 2:5]
4×4 SubDataFrame
 Row │ Age    Sex     Job    Housing
     │ Int64  String  Int64  String
─────┼───────────────────────────────
   1 │    22  female      2  own
   2 │    49  male        1  own
   3 │    45  male        2  free
   4 │    53  male        2  free
```

## Changing the Data Stored in a Data Frame

We make a subset of a `german` data frame and store it in `df1` data frame to show how to perform mutating operations.

```jldoctest dataframe
julia> df1 = german[1:6, 2:4]
6×3 DataFrame
 Row │ Age    Sex     Job
     │ Int64  String  Int64
─────┼──────────────────────
   1 │    67  male        2
   2 │    22  female      2
   3 │    49  male        1
   4 │    45  male        2
   5 │    53  male        2
   6 │    35  male        1
```

In the following example we replace the column `:Age` in our `df1`data frame:

```jldoctest dataframe
julia> val = [80, 85, 98, 95, 78, 89]
6-element Vector{Int64}:
 80
 85
 98
 95
 78
 89

julia> df1.Age = val
6-element Vector{Int64}:
 80
 85
 98
 95
 78
 89

julia> df1
6×3 DataFrame
 Row │ Age    Sex     Job
     │ Int64  String  Int64
─────┼──────────────────────
   1 │    80  male        2
   2 │    85  female      2
   3 │    98  male        1
   4 │    95  male        2
   5 │    78  male        2
   6 │    89  male        1
```

This is a non-copying operation. One can perform it only if `val` vector has the same length as number
of rows of `df1` or as a special case if `df1` would not have any columns. 

```jldoctest dataframe
julia> df1.Age === val # no copy is performed
true
```

Set values of column `:Job` in row `1:3` to values `[2, 4, 6]` *in-place* means write to an existing 
column in a data frame.

```jldoctest dataframe
julia> df1[1:3, :Job] = [2, 3, 2] # set value of `:Job` in row `1:3` to values `[2, 4, 6]` *in-place*
3-element Vector{Int64}:
 2
 3
 2

julia> df1
6×3 DataFrame
 Row │ Age    Sex     Job
     │ Int64  String  Int64
─────┼──────────────────────
   1 │    80  male        2
   2 │    85  female      3
   3 │    98  male        2
   4 │    95  male        2
   5 │    78  male        2
   6 │    89  male        1
```

We are replacing column `:Sex` with `["male", "female", "female", "transgender", "female", "male"]` 
without copying (with the exception that if it is an `AbstractRange` it gets converted to a Vector) 
and the syntax is like `df[!, col] = val` where `col` in any column name from our parent data frame.

```jldoctest dataframe
julia> df1[!, :Sex] = ["male", "female", "female", "transgender", "female", "male"]
6-element Vector{String}:
 "male"
 "female"
 "female"
 "transgender"
 "female"
 "male"

julia> df1
6×3 DataFrame
 Row │ Age    Sex          Job
     │ Int64  String       Int64
─────┼───────────────────────────
   1 │    80  male             2
   2 │    85  female           3
   3 │    98  female           2
   4 │    95  transgender      2
   5 │    78  female           2
   6 │    89  male             1
```

set row 3 of columns `:Age`, `:Sex`, and `:Job` in-place:

```jldoctest dataframe
julia> df1[3, 1:3] = [78, "male", 4] 
3-element Vector{Any}:
 78
   "male"
  4

julia> df1
6×3 DataFrame
 Row │ Age    Sex          Job
     │ Int64  String       Int64
─────┼───────────────────────────
   1 │    80  male             2
   2 │    85  female           3
   3 │    78  male             4
   4 │    95  transgender      2
   5 │    78  female           2
   6 │    89  male             1
```

Now, let us explain how `DataFrameRow` can be used to mutate its parent data frame: 

```jldoctest dataframe
julia> dfr = df1[2, :] # DataFrameRow with the second row and all columns of df1
DataFrameRow
 Row │ Age    Sex     Job
     │ Int64  String  Int64
─────┼──────────────────────
   2 │    85  female      3

julia> dfr.Age = 98 # set value of col `:Age` in row `2` to `98` in-place
98

julia> dfr
DataFrameRow
 Row │ Age    Sex     Job
     │ Int64  String  Int64
─────┼──────────────────────
   2 │    98  female      3

julia> dfr[2:3] = ["male", 2] # set values of entries in columns `:Sex` and `:Job` 
2-element Vector{Any}:
  "male"
 2

julia> dfr
DataFrameRow
 Row │ Age    Sex     Job
     │ Int64  String  Int64
─────┼──────────────────────
   2 │    98  male        2
```

`DataFrameRow` is a view into a single row of a data frame.
You can also create views into multiple rows of a data frame, which produce `SubDataFrame` objects.
To learn more about `view` please have a look to **Views** section of this tutorial. 

```jldoctest dataframe
julia> sdf = view(df1, :, 2:3) # Column subsetting
6×2 SubDataFrame
 Row │ Sex          Job
     │ String       Int64
─────┼────────────────────
   1 │ male             2
   2 │ male             2
   3 │ male             4
   4 │ transgender      2
   5 │ female           2
   6 │ male             1

julia> sdf[2, :Sex] = "female" # set value of col `:Sex` in second row to `female` in-place
"female"

julia> sdf
6×2 SubDataFrame
 Row │ Sex          Job
     │ String       Int64
─────┼────────────────────
   1 │ male             2
   2 │ female           2
   3 │ male             4
   4 │ transgender      2
   5 │ female           2
   6 │ male             1
```

Set value of multiple columns:

```jldoctest dataframe
julia> sdf[6, 1:2] = ["female", 3]
2-element Vector{Any}:
  "female"
 3

julia> sdf
6×2 SubDataFrame
 Row │ Sex          Job
     │ String       Int64
─────┼────────────────────
   1 │ male             2
   2 │ female           2
   3 │ male             4
   4 │ transgender      2
   5 │ female           2
   6 │ female           3
```

In the above examples we have talked about `SubDataFrame`. In the given example,
set the value of column `:Job` in our parent data frame `df1`:

```jldoctest dataframe
julia> df1[:, 3] = [4, 5, 7, 8, 2, 1]
6-element Vector{Int64}:
 4
 5
 7
 8
 2
 1

julia> df1
6×3 DataFrame
 Row │ Age    Sex          Job
     │ Int64  String       Int64
─────┼───────────────────────────
   1 │    80  male             4
   2 │    98  female           5
   3 │    78  male             7
   4 │    95  transgender      8
   5 │    78  female           2
   6 │    89  female           1
```

## Broadcasting

Apart from normal assignment one can do broadcasting assignment. The following broadcasting rules 
apply to `AbstractDataFrame` objects:

- `AbstractDataFrame` behaves in broadcasting like a two-dimensional collection compatible with matrices.
- If an `AbstractDataFrame` takes part in broadcasting then a `DataFrame` is always produced as a result. 
  In this case the requested broadcasting operation produces an object with exactly two dimensions. An 
  exception is when an `AbstractDataFrame` is used only as a source of broadcast assignment into an object 
  of dimensionality higher than two.
- If multiple `AbstractDataFrame` objects take part in broadcasting then they have to have identical column 
  names.

Note that if broadcasting assignment operation throws an error the target data frame may be partially changed 
so it is unsafe to use it afterwards (the column length correctness will be preserved).

In Julia base the standard rule to do [broadcasting](https://docs.julialang.org/en/v1/manual/mathematical-operations/#man-dot-operators) is to use `.`. For example, as opposed to `R` this operation fails:

```jldoctest dataframe
julia> s = [25, 26, 35, 56]
4-element Vector{Int64}:
 25
 26
 35
 56

julia> s[2: 3] = 0
ERROR: ArgumentError: indexed assignment with a single value to many locations is not supported; perhaps use broadcasting `.=` instead?
```

Instead we have to write:

```jldoctest dataframe
julia> s[2: 3] .= 0
2-element view(::Vector{Int64}, 2:3) with eltype Int64:
 0
 0

julia> s
4-element Vector{Int64}:
 25
  0
  0
 56
```

Similar syntax is fully supported in DataFrames.jl. Here, Column `:Age` is replaced freshly allocated 
vector because of broadcasting assignment:

```jldoctest dataframe
julia> df1[!, :Age] .= [85, 89, 78, 58, 96, 68] # col `:Age` is replaced freshly allocated vector
6-element Vector{Int64}:
 85
 89
 78
 58
 96
 68

julia> df1
6×3 DataFrame
 Row │ Age    Sex          Job
     │ Int64  String       Int64
─────┼───────────────────────────
   1 │    85  male             4
   2 │    89  female           5
   3 │    78  male             7
   4 │    58  transgender      8
   5 │    96  female           2
   6 │    68  female           1
```

Note that if columns `:Customers` and `:City` are not present in `df1` then using `!` and `:` are equivalent. The 
major difference between in-place and replace operations is that replacing columns is needed if new values have a 
different type than the old ones. For instance here `!` works and `:` fails:

```jldoctest dataframe
julia> df1[!, :Customers] .= ["Rohit", "Akshat", "Rahul", "Aayush", "Prateek", "Anam"] # allocates a new column `:Customers` and adds it
6-element Vector{String}:
 "Rohit"
 "Akshat"
 "Rahul"
 "Aayush"
 "Prateek"
 "Anam"

julia> df1[:, :City] .= ["Kanpur", "Lucknow", "Bhuvneshwar", "Jaipur", "Ranchi", "Dehradoon"] # allocates the column in-place because `:City` is not present in `data`
6-element Vector{String}:
 "Kanpur"
 "Lucknow"
 "Bhuvneshwar"
 "Jaipur"
 "Ranchi"
 "Dehradoon"

julia> df1
6×5 DataFrame
 Row │ Age    Sex          Job    Customers  City
     │ Int64  String       Int64  String     String
─────┼───────────────────────────────────────────────────
   1 │    85  male             4  Rohit      Kanpur
   2 │    89  female           5  Akshat     Lucknow
   3 │    78  male             7  Rahul      Bhuvneshwar
   4 │    58  transgender      8  Aayush     Jaipur
   5 │    96  female           2  Prateek    Ranchi
   6 │    68  female           1  Anam       Dehradoon
```

Assignment of a scalar to a data frame can be done in ranges using broadcasting:

```jldoctest dataframe
julia> df1[:, 3] .= 4 # an in-place replacement of values stored in column number 3 by 4
6-element view(::Vector{Int64}, :) with eltype Int64:
 4
 4
 4
 4
 4
 4

julia> df1
6×5 DataFrame
 Row │ Age    Sex          Job    Customers  City
     │ Int64  String       Int64  String     String
─────┼───────────────────────────────────────────────────
   1 │    85  male             4  Rohit      Kanpur
   2 │    89  female           4  Akshat     Lucknow
   3 │    78  male             4  Rahul      Bhuvneshwar
   4 │    58  transgender      4  Aayush     Jaipur
   5 │    96  female           4  Prateek    Ranchi
   6 │    68  female           4  Anam       Dehradoon
```

Here, `!` get us columns without copying and when setting columns it replaces them while 
`:` get us columns with copying and when setting columns it does this in-place. 

```jldoctest dataframe
julia> df1[:, :Age] .= "Economics"
ERROR: MethodError: Cannot `convert` an object of type String to an object of type Int64

julia> df1[!, :Age] .= "Economics"
6-element Vector{String}:
 "Economics"
 "Economics"
 "Economics"
 "Economics"
 "Economics"
 "Economics"

julia> df1
6×5 DataFrame
 Row │ Age        Sex          Job    Customers  City
     │ String     String       Int64  String     String
─────┼───────────────────────────────────────────────────────
   1 │ Economics  male             4  Rohit      Kanpur
   2 │ Economics  female           4  Akshat     Lucknow
   3 │ Economics  male             4  Rahul      Bhuvneshwar
   4 │ Economics  transgender      4  Aayush     Jaipur
   5 │ Economics  female           4  Prateek    Ranchi
   6 │ Economics  female           4  Anam       Dehradoon
```

In most cases above, as you can see, for getting a column or assigning to a column instead of `df1[!, :col]` 
and `df1[!, :col] = val` it is usually better to just write `df1.col` and `df1.col = val` respectively as 
it is the same and simpler to type and read.

However, there are some scenarios in `DataFrames.jl`, when we naturally want a broadcasting-like behaviour,
but do not allow for the use of `.` operation. These operations are based on `=>` syntax. Lets have a look on 
some examples:

Adding columns to the data frames. In this example we are creating a column `:Country` with function `insertcols!`
and then it is broadcasted to all rows in the output data frame:

```jldoctest dataframe
julia> insertcols!(df1, :Country => "India")
6×6 DataFrame
 Row │ Age    Sex          Job    Customers  City         Country
     │ Int64  String       Int64  String     String       String
─────┼────────────────────────────────────────────────────────────
   1 │    85  male             4  Rohit      kanpur       India
   2 │    89  female           4  Akshat     Lucknow      India
   3 │    78  male             4  Rahul      Bhuvneshwar  India
   4 │    58  transgender      4  Aayush     Jaipur       India
   5 │    96  female           4  Prateek    Ranchi       India
   6 │    68  female           4  Anam       Dehradoon    India
```

# Not, Between, Cols, and All column selectors

You can use `Not`, `Between`, `Cols`, and `All` selectors in more complex column selection
scenarioes. `All()` allows us to select all columns of `DataFrame` while `Between` selector 
allow us to specify a range of columns (we can specify the start and stop column using any 
of the single column selector syntaxes). On the other hand, `Not` selector allows us to specify 
the columns we want to exclude from the resulting data frames. We can put any valid other column 
selector inside `Not`. Finally `Cols(...)` selector picks a union of other selectors passed as 
its arguments.

A `Not` selector (from the [InvertedIndices](https://github.com/mbauman/InvertedIndices.jl) package)
can be used to select all columns excluding a specific subset:

```jldoctest dataframe
julia> german = copy(german_ref); # because in previous example we had done in-place replacement so our dataframe had changed

julia> german[:, Not(:Age)]
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

`Between()` selector allowing us to specify a range of columns (we can specify the start and 
stop column). In the below example we have specified columns `:Sex` and `:Housing` to start and 
stop respectively: 

```jldoctest dataframe
julia> german[:, Between(:Sex, :Housing)] # Columns starting from `:Sex` and ends at `:Housing`
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
```

`Cols` selector is picking a union of `Between()` selector passed as its arguments:

```jldoctest dataframe
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
```

In this example `Cols` selector is picking a union of `Not()` selector which is passed as an 
argument and `Not()` selector is allowing us to specify column `:Sex` we want to exclude from
the resulting data frame:

```jldoctest dataframe
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

In the above example `german[:, Cols("Age", Not("Sex"))]`, `:Age` column will come first after
removing the `:Sex` column. For more understanding if we will follow this example `german[:, Cols("Job", Not("Sex"))]`
then `:Job` column will be placed first and then the remaining order will be same after removing `:Sex` column.

You can also use `Regex` to select columns and `Not` for to select rows as in the example below:
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

## Using `combine`, `select`, `select!`, `transform`, and `transform!`

In DataFrames.jl we have five functions that we can use to perform transformations of columns 
of a data frame:
- `combine`: create a new data frame populated with columns that are results of transformation 
  applied to the source data frame columns, potentially combining its rows;
- `select`: create a new data frame that has the same number of rows as the source data frame 
  populated with columns that are results of transformations applied to the source data frame 
  columns; (the exception to the above number of rows invariant is `select(german)` which produces
  an empty data frame);
- `select!`: the same as `select` but updates the passed data frame in place;
- `transform`: the same as `select` but keeps the columns that were already present in the data frame
  (note though that these columns can be potentially modified by the transformation passed to `transform`);
- `transform!`: the same as `transform` but updates the passed data frame in place.

The general way to specify a transformation is:

- `source_column => transformation => target_column_name`
  In this scenario the `source_column` is passed as an argument to `transformation` and stored in `target_column_name`
  column. A `transformation` must be a callable (typically a function).
- `source_column => transformation`
  In this scenario we applied the transformation function to our `source_column` and then `target_column_name` will be 
  automatically generated.
- `source_column => target_column_name`
  we are renaming our `source_column` to `target_column_name`.

In this example, we are performing transformation using `select` and `combine`. In this 
scenario the source column `:Age` is passed as an argument to transformation function `mean` 
and stored in the target column name `:mean_age`. 

But we are observing that `select` produces as many rows in the produced data frame as there 
are rows in the source data frame, a single value is repeated accordingly but this is not the 
case for `combine`.

```jldoctest dataframe
julia> german = copy(german_ref);

julia> using Statistics

julia> select(german, :Age => mean => :mean_age) 
1000×1 DataFrame
  Row │ mean_age
      │ Float64
──────┼──────────
    1 │   35.546
    2 │   35.546
    3 │   35.546
    4 │   35.546
    5 │   35.546
    6 │   35.546
    7 │   35.546
    8 │   35.546
  ⋮   │    ⋮
  994 │   35.546
  995 │   35.546
  996 │   35.546
  997 │   35.546
  998 │   35.546
  999 │   35.546
 1000 │   35.546
 985 rows omitted

julia> combine(german, :Age => mean => :mean_age)
1×1 DataFrame
 Row │ mean_age
     │ Float64
─────┼──────────
   1 │   35.546
```

However, if other columns in `combine` would produce multiple rows the repetition also happens:

```jldcotest dataframe
julia> combine(german, :Age => mean => :mean_age, :Housing => unique => :housing)
3×2 DataFrame
 Row │ mean_age  housing
     │ Float64   String
─────┼───────────────────
   1 │   35.546  own
   2 │   35.546  free
   3 │   35.546  rent
```

Note, however, that it is not allowed to return vectors of different lengths in different transformations:

```jldoctest dataframe
julia> combine(german, :Age, :Housing => unique => :Housing)
ERROR: ArgumentError: New columns must have the same length as old columns
```

Several values that can be returned by a transformation are treated to produce multiple columns by default. 
Therefore they are not allowed to be returned from a function unless `AsTable` or multiple target column names
are specified. Let us see an example:

```jldoctest dataframe
julia> combine(german, :Age => x -> (Age=x, Age2 = x.^2))
ERROR: ArgumentError: Table returned but a single output column was expected

julia> combine(german, :Age => (x -> (Age=x, Age2 = x.^2)) => AsTable)
1000×2 DataFrame
  Row │ Age    Age2
      │ Int64  Int64
──────┼──────────────
    1 │    67   4489
    2 │    22    484
    3 │    49   2401
    4 │    45   2025
    5 │    53   2809
    6 │    35   1225
    7 │    53   2809
    8 │    35   1225
  ⋮   │   ⋮      ⋮
  994 │    30    900
  995 │    50   2500
  996 │    31    961
  997 │    40   1600
  998 │    38   1444
  999 │    23    529
 1000 │    27    729
     985 rows omitted
```

Let us discuss some other examples using `select`. Often we want to apply some function not to the whole
column of a data frame, but rather to its individual elements. Normally we can achieve this using broadcasting
like this:

```jldoctest dataframe
julia> select(german, :Sex => (x -> uppercase.(x)) => :Sex)
1000×1 DataFrame
  Row │ Sex
      │ String
──────┼────────
    1 │ MALE
    2 │ FEMALE
    3 │ MALE
    4 │ MALE
    5 │ MALE
    6 │ MALE
    7 │ MALE
    8 │ MALE
  ⋮   │   ⋮
  994 │ MALE
  995 │ MALE
  996 │ FEMALE
  997 │ MALE
  998 │ MALE
  999 │ MALE
 1000 │ MALE
985 rows omitted

julia> select(german, :Sex => ByRow(uppercase) => :SEX) # `ByRow` convenience wrapper for a function that creates its broadcasted variant
1000×1 DataFrame
  Row │ SEX
      │ String
──────┼────────
    1 │ MALE
    2 │ FEMALE
    3 │ MALE
    4 │ MALE
    5 │ MALE
    6 │ MALE
    7 │ MALE
    8 │ MALE
  ⋮   │   ⋮
  994 │ MALE
  995 │ MALE
  996 │ FEMALE
  997 │ MALE
  998 │ MALE
  999 │ MALE
 1000 │ MALE
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

We can skip specifying a target column name, in which case it is generated automatically by suffixing source
column name by function name that is applied to it. For example:

```jldoctest dataframe
julia> select(german, :Sex => ByRow(uppercase))
1000×1 DataFrame
  Row │ Sex_uppercase
      │ String
──────┼───────────────
    1 │ MALE
    2 │ FEMALE
    3 │ MALE
    4 │ MALE
    5 │ MALE
    6 │ MALE
    7 │ MALE
    8 │ MALE
  ⋮   │       ⋮
  994 │ MALE
  995 │ MALE
  996 │ FEMALE
  997 │ MALE
  998 │ MALE
  999 │ MALE
 1000 │ MALE
      985 rows omitted

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

julia> select(german, r"Sex") # select columns containing 'Sex' string
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

julia> select(german, r"Sex", "Job") # Selects more than one column
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
```

In the above example `ByRow` type is a special type used for selection operations to signal that
the wrapped function should be applied to each element (row) of the selection.

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

By default `select` copies columns of a passed source data frame. In order to avoid copying, pass `copycols=false`:

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

julia> df.Sex === german.Sex # copy
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

julia> df.Sex === german.Sex # no-copy is performed
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

`transform` and `transform!` functions work identically to `select` and `select!` with the only difference that
they retain all columns that are present in the source data frame and another difference is that `transform` and
`transform!` always copy columns when column renaming transformation is passed.

```jldoctest dataframe
julia> german = copy(german_ref);

julia> df = german_ref[1:8, 1:5]
8×5 DataFrame
 Row │ id     Age    Sex     Job    Housing
     │ Int64  Int64  String  Int64  String
─────┼──────────────────────────────────────
   1 │     0     67  male        2  own
   2 │     1     22  female      2  own
   3 │     2     49  male        1  own
   4 │     3     45  male        2  free
   5 │     4     53  male        2  free
   6 │     5     35  male        1  free
   7 │     6     53  male        2  own
   8 │     7     35  male        3  rent

julia> transform(df, :Age => maximum)
8×6 DataFrame
 Row │ id     Age    Sex     Job    Housing  Age_maximum
     │ Int64  Int64  String  Int64  String   Int64
─────┼───────────────────────────────────────────────────
   1 │     0     67  male        2  own               67
   2 │     1     22  female      2  own               67
   3 │     2     49  male        1  own               67
   4 │     3     45  male        2  free              67
   5 │     4     53  male        2  free              67
   6 │     5     35  male        1  free              67
   7 │     6     53  male        2  own               67
   8 │     7     35  male        3  rent              67

julia> transform(german, :Age => :Sex, :Sex => :Age) # swapping the value of `:Age` column with `:Sex` column
1000×10 DataFrame
  Row │ id     Age     Sex    Job    Housing  Saving accounts  Checking accoun ⋯
      │ Int64  String  Int64  Int64  String   String           String          ⋯
──────┼─────────────────────────────────────────────────────────────────────────
    1 │     0  male       67      2  own      NA               little          ⋯
    2 │     1  female     22      2  own      little           moderate
    3 │     2  male       49      1  own      little           NA
    4 │     3  male       45      2  free     little           little
    5 │     4  male       53      2  free     little           little          ⋯
    6 │     5  male       35      1  free     NA               NA
    7 │     6  male       53      2  own      quite rich       NA
    8 │     7  male       35      3  rent     little           moderate
  ⋮   │   ⋮      ⋮       ⋮      ⋮       ⋮            ⋮                ⋮        ⋱
  994 │   993  male       30      3  own      little           little          ⋯
  995 │   994  male       50      2  own      NA               NA
  996 │   995  female     31      1  own      little           NA
  997 │   996  male       40      3  own      little           little
  998 │   997  male       38      2  own      little           NA              ⋯
  999 │   998  male       23      2  free     little           little
 1000 │   999  male       27      2  own      moderate         moderate
                                                  4 columns and 985 rows omitted

julia> df1 = german[:, [:Age, :Job]] # getting two columns
1000×2 DataFrame
  Row │ Age    Job
      │ Int64  Int64
──────┼──────────────
    1 │    67      2
    2 │    22      2
    3 │    49      1
    4 │    45      2
    5 │    53      2
    6 │    35      1
    7 │    53      2
    8 │    35      3
  ⋮   │   ⋮      ⋮
  994 │    30      3
  995 │    50      2
  996 │    31      1
  997 │    40      3
  998 │    38      2
  999 │    23      2
 1000 │    27      2
     985 rows omitted

julia> transform(df1, [:Age, :Job] => (+) => :res) # put the result of `:Age` and `:Job` column after addition in `:res` column
1000×3 DataFrame
  Row │ Age    Job    res
      │ Int64  Int64  Int64
──────┼─────────────────────
    1 │    67      2     69
    2 │    22      2     24
    3 │    49      1     50
    4 │    45      2     47
    5 │    53      2     55
    6 │    35      1     36
    7 │    53      2     55
    8 │    35      3     38
  ⋮   │   ⋮      ⋮      ⋮
  994 │    30      3     33
  995 │    50      2     52
  996 │    31      1     32
  997 │    40      3     43
  998 │    38      2     40
  999 │    23      2     25
 1000 │    27      2     29
            985 rows omitted                                  
```
