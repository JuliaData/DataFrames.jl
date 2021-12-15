# First Steps with DataFrames.jl

**The tutorial section of the manual is still work in progress. Please report any questions or comments as issues in DataFrames.jl GitHub repository. Thank you!**

## Setting up the Environment

If want to use the DataFrames.jl package you need to install it first.
You can do it using the following commands:

```julia
julia> using Pkg

julia> Pkg.add("DataFrames")
```

or

```julia
julia> ] # ']' should be pressed

(@v1.6) pkg> add DataFrames
```

If you want to make sure everything works as expected you can run the tests bundled
with DataFrames.jl, but be warned that it will take more than 30 minutes:

```julia
julia> using Pkg

julia> Pkg.test("DataFrames") # Warning! This will take more than 30 minutes.
```

Additionally, it is recommended to check the version of DataFrames.jl that
you have installed with the `status` command.

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

## Constructors and Basic Utility Functions

### Constructors

In this section you will see several ways to create a `DataFrame` using the constructor.
You can find a detailed list of supported constructors along with more examples
in the documentation of the [`DataFrame`](@ref) object.

We start by creating an empty `DataFrame`:

```jldoctest dataframe
julia> DataFrame()
0×0 DataFrame
```

Now let us initialize a `DataFrame` with several columns. This is a basic way to do it is the following:

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

Observe that using this constructor scalars, like `1` for the column `:fixed` get automatically broadcasted
to fill all rows of the created `DataFrame`.

Sometimes one needs to create a data frame whose column names are not valid Julia identifiers.
In such a case the following form, where `=` is replaced by `=>` is handy:

```jldoctest dataframe
julia> DataFrame("customer age" => [15, 20, 25],
                 "first name" => ["Rohit", "Rahul", "Akshat"])
3×2 DataFrame
 Row │ customer age  first name
     │ Int64         String
─────┼──────────────────────────
   1 │           15  Rohit
   2 │           20  Rahul
   3 │           25  Akshat
```

Notice that this time we have passed column names as strings.

Often you have your source data stored in a dictionary.
Provided that the keys of the dictionary are strings or `Symbol`s
you can also easily create a `DataFrame` from it:

```jldoctest dataframe
julia> dict = Dict("customer age" => [15, 20, 25],
                   "first name" => ["Rohit", "Rahul", "Akshat"])
Dict{String, Vector} with 2 entries:
  "first name"   => ["Rohit", "Rahul", "Akshat"]
  "customer age" => [15, 20, 25]

julia> DataFrame(dict)
3×2 DataFrame
 Row │ customer age  first name
     │ Int64         String
─────┼──────────────────────────
   1 │           15  Rohit
   2 │           20  Rahul
   3 │           25  Akshat

julia> dict = Dict(:customer_age => [15, 20, 25],
                   :first_name => ["Rohit", "Rahul", "Akshat"])
Dict{Symbol, Vector} with 2 entries:
  :customer_age => [15, 20, 25]
  :first_name   => ["Rohit", "Rahul", "Akshat"]

julia> DataFrame(dict)
3×2 DataFrame
 Row │ customer_age  first_name
     │ Int64         String
─────┼──────────────────────────
   1 │           15  Rohit
   2 │           20  Rahul
   3 │           25  Akshat
```

Using `Symbol`s, e.g. `:customer_age` rather than strings, e.g. `"customer age"`
to denote column names is preferred as it is faster. However, as you can see
in the example above if our column name contains a space it is not very
convenient to pass it as a `Symbol` (you would have to write `Symbol("customer age")`,
which is verbose) so using a string is more convenient.

It is also quite common to create a `DataFrame` from a `NamedTuple` of vectors
or a vector of `NamedTuple`s. Here are some examples of these operations:

```jldoctest dataframe
julia> DataFrame((a=[1, 2], b=[3, 4]))
2×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      3
   2 │     2      4

julia> DataFrame([(a=1, b=0), (a=2, b=0)])
2×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      0
   2 │     2      0
```

Let us finish our review of constructors by showing how to create a `DataFrame`
from a matrix. In this case you pass a matrix as a first argument. If the second
argument is just `:auto` then column names `x1`, `x2`, ... will be auto generated.

```jldoctest dataframe
julia> DataFrame([1 0; 2 0], :auto)
2×2 DataFrame
 Row │ x1     x2
     │ Int64  Int64
─────┼──────────────
   1 │     1      0
   2 │     2      0
```

Alternatively you can pass a vector of column names as a second argument to the
`DataFrame` constructor:

```jldoctest dataframe
julia> mat = [1 2 4 5; 15 58 69 41; 23 21 26 69]
3×4 Matrix{Int64}:
  1   2   4   5
 15  58  69  41
 23  21  26  69

julia> nms = ["a", "b", "c", "d"]
4-element Vector{String}:
 "a"
 "b"
 "c"
 "d"

julia> DataFrame(mat, nms)
3×4 DataFrame
 Row │ a      b      c      d
     │ Int64  Int64  Int64  Int64
─────┼────────────────────────────
   1 │     1      2      4      5
   2 │    15     58     69     41
   3 │    23     21     26     69
```

You now know how to create a `DataFrame` from data that you already have in your
Julia session. In the next section we show how to load data to a `DataFrame`
from disk.

### Reading Data From CSV Files

Here we focus on one of the most common scenarios, where one has data stored on
disk in the CSV format.

First make sure you have CSV.jl installed. You can do it using the following
instructions:

```julia
julia> using Pkg

julia> Pkg.add("CSV")
```

In order to read the file in we will use the `CSV.read` function.

```jldoctest dataframe
julia> using CSV

julia> german_ref = CSV.read(joinpath(dirname(pathof(DataFrames)),
                                      "..", "docs", "src", "assets", "german.csv"),
                             DataFrame)
1000×10 DataFrame
  Row │ id     Age    Sex      Job    Housing  Saving accounts  Checking accou ⋯
      │ Int64  Int64  String7  Int64  String7  String15         String15       ⋯
──────┼─────────────────────────────────────────────────────────────────────────
    1 │     0     67  male         2  own      NA               little         ⋯
    2 │     1     22  female       2  own      little           moderate
    3 │     2     49  male         1  own      little           NA
    4 │     3     45  male         2  free     little           little
    5 │     4     53  male         2  free     little           little         ⋯
    6 │     5     35  male         1  free     NA               NA
    7 │     6     53  male         2  own      quite rich       NA
    8 │     7     35  male         3  rent     little           moderate
  ⋮   │   ⋮      ⋮       ⋮       ⋮       ⋮            ⋮                ⋮       ⋱
  994 │   993     30  male         3  own      little           little         ⋯
  995 │   994     50  male         2  own      NA               NA
  996 │   995     31  female       1  own      little           NA
  997 │   996     40  male         3  own      little           little
  998 │   997     38  male         2  own      little           NA             ⋯
  999 │   998     23  male         2  free     little           little
 1000 │   999     27  male         2  own      moderate         moderate
                                                  4 columns and 985 rows omitted
```

As you can see the data frame is wider and taller than the display width, so it
got cropped and its 4 rightmost columns and middle 985 rows were not printed.
Later in the tutorial we will discuss how to force Julia to show the whole
data frame if we wanted so.

Also observe that DataFrames.jl displays the data type of the column
below its name. In our case, it is an `Int64`, or `String7` and `String15`.

Let us mention here the difference between the standard `String` type in Julia
and e.g. the `String7` or `String15` types. The types with number suffix denote
strings that have a fixed width (similar `CHAR(N)` type provided by many data
bases). Such strings are much faster to work with (especially if you have many
of them) than the standard `String` type because their instances are not heap
allocated. For this reason `CSV.read` by default reads in narrow string columns
using these fixed-width types.

Let us now explain in detail the following code block:
```julia
german_ref = CSV.read(joinpath(dirname(pathof(DataFrames)),
                               "..", "docs", "src", "assets", "german.csv"),
                      DataFrame)
```
- we are storing the `german.csv` file in the DataFrames.jl repository to make user's life easier and
  avoid having to download it each time;
- `pathof(DataFrames)` gives us the full path of the file that was used to import the DataFrames.jl package;
- first we split the directory part from it using `dirname`;
- then from this directory we need to move to the directory where the `german.csv` file is stored; we use
  `joinpath` as this is a recommended way to compose paths to resources stored on disk in an operating
  system independent way (remember that Widnows and Unix differ as they use either `/` or `\` as path
  separator; the `joinpath` function ensures we are not running into issues with this);
- then we read the CSV file; the second argument to `CSV.read` is `DataFrame` to indicate that we want to
  read in the file into a `DataFrame` (as `CSV.read` allows for many different target formats of data it
  can read-into).

Before proceeding copy the reference data frame:

```jldoctest dataframe
julia> german = copy(german_ref); # we copy the data frame
```

In this way we can always easily restore our data even if we mess up the
`german` data frame by modifying it.

### Basic Operations on Data Frames

To access the columns of a data frame directly (i.e. without copying) you can use
one of the following syntaxes:
`german.Sex`, `german."Sex"`, `german[!, :Sex]` or `german[!, "Sex"]`.

The two latter syntaxes using indexing are more flexible as they allow
us passing a variable holding the name of the column, and not only a literal name
as in the case of the syntax using a `.`.

```jldoctest dataframe
julia> german.Sex
1000-element PooledArrays.PooledVector{String7, UInt32, Vector{UInt32}}:
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

julia> colname = "Sex"
"Sex"

julia> german[!, colname]
1000-element PooledArrays.PooledVector{String7, UInt32, Vector{UInt32}}:
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

Since `german.Sex` does not make a copy, changing the elements of the column vector returned by this
operation will affect the values stored in the original `german` data frame. To get a *copy*
of the column you can use `german[:, :Sex]` or `german[:, "Sex"]`.
In this case changing the vector returned by this operation does not affect the data stored in the `german` data frame.

The `===` function allows us to check if both expressions produce the same object
and confirm the behavior described above:

```jldoctest dataframe
julia> german.Sex === german[!, :Sex]
true

julia> german.Sex === german[:, :Sex]
false
```

You can obtain a vector of column names of the data frame as `String`s using the `names` function:

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

Sometimes you are interested in names of columns that meet a particular condition.

For example you can get column names with a given element type by passing this
type as a second argument to the `names` function:

```jldoctest dataframe
julia> names(german, AbstractString)
5-element Vector{String}:
 "Sex"
 "Housing"
 "Saving accounts"
 "Checking account"
 "Purpose"
```

You can explore more options of filtering column names in the documentation of
the [`names`](@ref) function.

If instead you wanted to get column names of a data frame as `Symbol`s use the `propertynames` function:

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

As you can see the column names containing spaces are not very convenient to work with
as `Symbol`s because they require more typing and introduce some visual noise.

If you were interested in element types of the columns instead. You can use the
`eachcol(german)` function to get an iterator over the columns of the data frame.
Then you can broadcast the `eltype` function over it to get the desired result:

```jldoctest dataframe
julia> eltype.(eachcol(german))
10-element Vector{DataType}:
 Int64
 Int64
 String7
 Int64
 String7
 String15
 String15
 Int64
 Int64
 String31
```

!!! note

    Remember that DataFrames.jl allows to use `Symbol`s (like `:id`) and strings
    (like `"id"`) for all column indexing operations for convenience.
    However, using `Symbol`s is slightly faster, but strings are simpler to work
    with when non standard characters are present in column names or one wants
    to manipulate them.

Before we wrap up let us discuss the `empty` and `empty!` functions that
To remove all rows from a `DataFrame`. Understanding the difference between the
behavior of these two functions will help you to understand the function naming
scheme in DataFrames.jl in general.

Let us start with the example of using the `empty` and `empty!` functions:

```jldoctest dataframe
julia> empty(german)
0×10 DataFrame

julia> german
1000×10 DataFrame
  Row │ id     Age    Sex      Job    Housing  Saving accounts  Checking accou ⋯
      │ Int64  Int64  String7  Int64  String7  String15         String15       ⋯
──────┼─────────────────────────────────────────────────────────────────────────
    1 │     0     67  male         2  own      NA               little         ⋯
    2 │     1     22  female       2  own      little           moderate
    3 │     2     49  male         1  own      little           NA
    4 │     3     45  male         2  free     little           little
    5 │     4     53  male         2  free     little           little         ⋯
    6 │     5     35  male         1  free     NA               NA
    7 │     6     53  male         2  own      quite rich       NA
    8 │     7     35  male         3  rent     little           moderate
  ⋮   │   ⋮      ⋮       ⋮       ⋮       ⋮            ⋮                ⋮       ⋱
  994 │   993     30  male         3  own      little           little         ⋯
  995 │   994     50  male         2  own      NA               NA
  996 │   995     31  female       1  own      little           NA
  997 │   996     40  male         3  own      little           little
  998 │   997     38  male         2  own      little           NA             ⋯
  999 │   998     23  male         2  free     little           little
 1000 │   999     27  male         2  own      moderate         moderate
                                                  4 columns and 985 rows omitted

julia> empty!(german)
0×10 DataFrame

julia> german
0×10 DataFrame
```

In the above example `empty` function created a new `DataFrame` with the same column names
and column element types as `german` but with zero rows. On the other hand `empty!` function
removed all rows from `german` in-place and made each of its columns empty.

The difference between the behavior of the `empty` and `empty!` functions is an application of the
[stylistic convention](https://docs.julialang.org/en/v1/manual/variables/#Stylistic-Conventions)
employed in the Julia language.
This convention is followed in all functions provided by the DataFrames.jl package.

### Getting Basic Information about a Data Frame

In this section we will learn about how to get basic information on our `german` `DataFrame`:

The `size` function returns the dimensions of the data frame.
First we restore the `german` data frame, as we have just emptied it above.

```jldoctest dataframe
julia> german = copy(german_ref);

julia> size(german)
(1000, 10)

julia> size(german, 1)
1000

julia> size(german, 2)
10
```

Additionally the `nrow` and `ncol` functions can be used to get the number of rows
and columns in a data frame:
```jldoctest dataframe
julia> nrow(german)
1000

julia> ncol(german)
10
```

To get basic statistics of data in your data frame use the `describe` function
(check out the help of [`describe`](@ref) for information on how to customize
the shown statistics).

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

To limit the columns processed by `desribe` use `cols` keyword argument, e.g.:

```jldoctest dataframe
julia> describe(german, cols=1:3)
3×7 DataFrame
 Row │ variable  mean    min     median  max   nmissing  eltype
     │ Symbol    Union…  Any     Union…  Any   Int64     DataType
─────┼────────────────────────────────────────────────────────────
   1 │ id        499.5   0       499.5   999          0  Int64
   2 │ Age       35.546  19      33.0    75           0  Int64
   3 │ Sex               female          male         0  String7
```

The default statistics reported are mean, min, median, max, number of missing values, and element type of
the column. `missing` values are skipped when computing the summary statistics.

You can adjust how data frame is displayed by calling the `show` function manually:
`show(german, allrows=true)` prints all rows even if they do not fit on screen and
`show(german, allcols=true)` does the same for columns, e.g.:

```jldoctest dataframe
julia> show(german, allcols=true)
1000×10 DataFrame
  Row │ id     Age    Sex      Job    Housing  Saving accounts  Checking account  Credit amount  Duration  Purpose
      │ Int64  Int64  String7  Int64  String7  String15         String15          Int64          Int64     String31
──────┼────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
    1 │     0     67  male         2  own      NA               little                     1169         6  radio/TV
    2 │     1     22  female       2  own      little           moderate                   5951        48  radio/TV
    3 │     2     49  male         1  own      little           NA                         2096        12  education
    4 │     3     45  male         2  free     little           little                     7882        42  furniture/equipment
    5 │     4     53  male         2  free     little           little                     4870        24  car
    6 │     5     35  male         1  free     NA               NA                         9055        36  education
    7 │     6     53  male         2  own      quite rich       NA                         2835        24  furniture/equipment
    8 │     7     35  male         3  rent     little           moderate                   6948        36  car
  ⋮   │   ⋮      ⋮       ⋮       ⋮       ⋮            ⋮                ⋮                ⋮           ⋮               ⋮
  994 │   993     30  male         3  own      little           little                     3959        36  furniture/equipment
  995 │   994     50  male         2  own      NA               NA                         2390        12  car
  996 │   995     31  female       1  own      little           NA                         1736        12  furniture/equipment
  997 │   996     40  male         3  own      little           little                     3857        30  car
  998 │   997     38  male         2  own      little           NA                          804        12  radio/TV
  999 │   998     23  male         2  free     little           little                     1845        45  radio/TV
 1000 │   999     27  male         2  own      moderate         moderate                   4576        45  car
                                                                                                               985 rows omitted
```

It is easy to compute descriptive statistics directly on individual columns using
the functions defined in the `Statistics` module:

```jldoctest dataframe
julia> using Statistics

julia> mean(german.Age)
35.546
```

If instead we want to apply some function to all columns of a data frame we can
use the `mapcols` function. It returns a `DataFrame` where each column of the source
data frame is transformed using a function passed as a first argument.
Note that `mapcols` guarantees not to reuse the columns from `german` in the returned `DataFrame`.
If the transformation returns its argument then it gets copied before being stored.

```jldoctest dataframe
julia> mapcols(id -> id .^ 2, german)
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

If you want to look at first and last rows of a data frame then you can do this
using the `first` and `last` functions respectively:

```jldoctest dataframe
julia> first(german, 6)
6×10 DataFrame
 Row │ id     Age    Sex      Job    Housing  Saving accounts  Checking accoun ⋯
     │ Int64  Int64  String7  Int64  String7  String15         String15        ⋯
─────┼──────────────────────────────────────────────────────────────────────────
   1 │     0     67  male         2  own      NA               little          ⋯
   2 │     1     22  female       2  own      little           moderate
   3 │     2     49  male         1  own      little           NA
   4 │     3     45  male         2  free     little           little
   5 │     4     53  male         2  free     little           little          ⋯
   6 │     5     35  male         1  free     NA               NA
                                                               4 columns omitted

julia> last(german, 5)
5×10 DataFrame
 Row │ id     Age    Sex      Job    Housing  Saving accounts  Checking accoun ⋯
     │ Int64  Int64  String7  Int64  String7  String15         String15        ⋯
─────┼──────────────────────────────────────────────────────────────────────────
   1 │   995     31  female       1  own      little           NA              ⋯
   2 │   996     40  male         3  own      little           little
   3 │   997     38  male         2  own      little           NA
   4 │   998     23  male         2  free     little           little
   5 │   999     27  male         2  own      moderate         moderate        ⋯
                                                               4 columns omitted
```

Using `first` and `last` without passing the number of rows will return a first/last
`DataFrameRow` in the data frame. `DataFrameRow` is a view into a single row of an
`AbstractDataFrame`. It stores a reference to a parent
`DataFrame` and information about which row and columns from the parent are selected.
You can think of `DataFrameRow` as a `NamedTuple` that is mutable,
i.e. allows to update the source data frame, which is often useful.

```jldoctest dataframe
julia> first(german)
DataFrameRow
 Row │ id     Age    Sex      Job    Housing  Saving accounts  Checking accoun ⋯
     │ Int64  Int64  String7  Int64  String7  String15         String15        ⋯
─────┼──────────────────────────────────────────────────────────────────────────
   1 │     0     67  male         2  own      NA               little          ⋯
                                                               4 columns omitted

julia> last(german)
DataFrameRow
  Row │ id     Age    Sex      Job    Housing  Saving accounts  Checking accou ⋯
      │ Int64  Int64  String7  Int64  String7  String15         String15       ⋯
──────┼─────────────────────────────────────────────────────────────────────────
 1000 │   999     27  male         2  own      moderate         moderate       ⋯
                                                               4 columns omitted
```

## Getting and Setting Data in a Data Frame

### Indexing Syntax

Data frame can be indexed in a similar way to matrices.
In the [Indexing](@ref) section of the manual you can find all details about all
the available options. Here we highlight the basic ones.

The general syntax for indexing is `data_frame[selected_rows, selected_columns]`. Observe that, as
opposed to matrices in Julia Base, it is required to always pass both row and column selector.
The colon `:` indicates that all items (rows or columns depending on its position) should be retained.
Here are a few examples:

```jldoctest dataframe
julia> german[1:5, [:Sex, :Age]]
5×2 DataFrame
 Row │ Sex      Age
     │ String7  Int64
─────┼────────────────
   1 │ male        67
   2 │ female      22
   3 │ male        49
   4 │ male        45
   5 │ male        53

julia> german[1:5, :]
5×10 DataFrame
 Row │ id     Age    Sex      Job    Housing  Saving accounts  Checking accoun ⋯
     │ Int64  Int64  String7  Int64  String7  String15         String15        ⋯
─────┼──────────────────────────────────────────────────────────────────────────
   1 │     0     67  male         2  own      NA               little          ⋯
   2 │     1     22  female       2  own      little           moderate
   3 │     2     49  male         1  own      little           NA
   4 │     3     45  male         2  free     little           little
   5 │     4     53  male         2  free     little           little          ⋯
                                                               4 columns omitted

julia> german[[1, 6, 15], :]
3×10 DataFrame
 Row │ id     Age    Sex      Job    Housing  Saving accounts  Checking accoun ⋯
     │ Int64  Int64  String7  Int64  String7  String15         String15        ⋯
─────┼──────────────────────────────────────────────────────────────────────────
   1 │     0     67  male         2  own      NA               little          ⋯
   2 │     5     35  male         1  free     NA               NA
   3 │    14     28  female       2  rent     little           little
                                                               4 columns omitted

julia> german[:, [:Age, :Sex]]
1000×2 DataFrame
  Row │ Age    Sex
      │ Int64  String7
──────┼────────────────
    1 │    67  male
    2 │    22  female
    3 │    49  male
    4 │    45  male
    5 │    53  male
    6 │    35  male
    7 │    53  male
    8 │    35  male
  ⋮   │   ⋮       ⋮
  994 │    30  male
  995 │    50  male
  996 │    31  female
  997 │    40  male
  998 │    38  male
  999 │    23  male
 1000 │    27  male
       985 rows omitted
```

Pay attention that `german[!, [:Sex]]` and `german[:, [:Sex]]` returns a data frame object,
while `german[!, :Sex]` and `german[:, :Sex]` returns a vector. In the first case, `[:Sex]`
is a vector, indicating that the resulting object should be a data frame. On the other hand,
`:Sex` is a single `Symbol`, indicating that a single column vector should be extracted. Note
that in the first case a vector is required to be passed (not just any iterable), so e.g.
`german[:, (:Age, :Sex)]` is not allowed, but `german[:, [:Age, :Sex]]` is valid.
Below we show both operations to highlight this difference:

```jldoctest dataframe
julia> german[!, [:Sex]]
1000×1 DataFrame
  Row │ Sex
      │ String7
──────┼─────────
    1 │ male
    2 │ female
    3 │ male
    4 │ male
    5 │ male
    6 │ male
    7 │ male
    8 │ male
  ⋮   │    ⋮
  994 │ male
  995 │ male
  996 │ female
  997 │ male
  998 │ male
  999 │ male
 1000 │ male
985 rows omitted

julia> german[!, :Sex]
1000-element PooledArrays.PooledVector{String7, UInt32, Vector{UInt32}}:
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

As it was explained earler in this tutorial the difference between using `!` and `:`
when passing a row index is that `!` does not perform a copy of columns, while `:` does.
Therefore `german[!, [:Sex]]` data frame stores the same vector as the source `german` data frame,
while `german[:, [:Sex]]` stores its copy.

The `!` selector normally should be avoided as using it can lead to hard to catch bugs.
However, when working with very large data frames
it can be useful to save memory and improve performance of operations.

Recapping what we have already learned,
To get the column `:Age` from the `german` data frame you can do the following:

- to copy the vector: `german[:, :Age]`, `german[:, "Age"]` or `german[:, 2]`;
- to get a vector without copying: `german.Age`, `german."Age"`, `german[!, :Age]`,
  `german[!, "Age"]` or `german[!, 2]`.

To get the first two columns as a `DataFrame`, we can index as follows:
- to get the copied columns: `german[:, 1:2]`, `german[:, [:id, :Age]]`, or `german[:, ["id", "Age"]]`;
- to reuse the columns without copying: `german[!, 1:2]`, `german[!, [:id, :Age]]`, or `german[!, ["id", "Age"]]`.

If you want to can get a single cell of a data frame use the same syntax as the one that gets a cell of a matrix:

```jldoctest dataframe
julia> german[4, 4]
2
```

### Views

We can also create a `view` of a data frame. It is often useful as it is more memory
efficient than creating a materialized selection. You can create it using a `view` function:

```jldoctest dataframe
julia> view(german, :, 2:5)
1000×4 SubDataFrame
  Row │ Age    Sex      Job    Housing
      │ Int64  String7  Int64  String7
──────┼────────────────────────────────
    1 │    67  male         2  own
    2 │    22  female       2  own
    3 │    49  male         1  own
    4 │    45  male         2  free
    5 │    53  male         2  free
    6 │    35  male         1  free
    7 │    53  male         2  own
    8 │    35  male         3  rent
  ⋮   │   ⋮       ⋮       ⋮       ⋮
  994 │    30  male         3  own
  995 │    50  male         2  own
  996 │    31  female       1  own
  997 │    40  male         3  own
  998 │    38  male         2  own
  999 │    23  male         2  free
 1000 │    27  male         2  own
                       985 rows omitted
```

or using a `@view` macro:

```jldoctest dataframe
julia> @view german[end:-1:1, [1, 4]]
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

Similarly we can get a view of one column of a data frame:

```jldoctest dataframe
julia> @view german[1:5, 1]
5-element view(::Vector{Int64}, 1:5) with eltype Int64:
 0
 1
 2
 3
 4
```

its single cell:

```jldoctest dataframe
julia> @view german[2, 2]
0-dimensional view(::Vector{Int64}, 2) with eltype Int64:
22
```

or a single row:

```jldoctest dataframe
julia> @view german[3, 2:5]
DataFrameRow
 Row │ Age    Sex      Job    Housing
     │ Int64  String7  Int64  String7
─────┼────────────────────────────────
   3 │    49  male         1  own
```

As you can see the row and column indexing syntax is exactly the same as for indexing.
The only difference is that we do not create a new object, but a view into an existing one.

In order to compare the performance of indexing vs creation of a view let us
run the following benchmark using the BenchmarkTools.jl package (please install
it if you want to re-run this comparison):

```julia
julia> using BenchmarkTools

julia> @btime $german[1:end-1, 1:end-1];
  9.900 μs (44 allocations: 57.56 KiB)

julia> @btime @view $german[1:end-1, 1:end-1];
  67.332 ns (2 allocations: 32 bytes)
```

As you can see creation of a view is:
- an order of magnitude faster;
- allocates much less memory.

The downside of the view is that:
- it points to the same memory as its parent (so changing a view changes the parent, which is sometimes undesirable);
- some operations might be a bit slower (as DataFrames.jl needs to perform a mapping of indices of a view to indices of the parent).

### Changing the Data Stored in a Data Frame

In order to show how to perform mutating operations on a data frame we make a subset of a `german` data frame first:

```jldoctest dataframe
julia> df1 = german[1:6, 2:4]
6×3 DataFrame
 Row │ Age    Sex      Job
     │ Int64  String7  Int64
─────┼───────────────────────
   1 │    67  male         2
   2 │    22  female       2
   3 │    49  male         1
   4 │    45  male         2
   5 │    53  male         2
   6 │    35  male         1
```

In the following example we replace the column `:Age` in our `df1` data frame
with a new vector:

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
 Row │ Age    Sex      Job
     │ Int64  String7  Int64
─────┼───────────────────────
   1 │    80  male         2
   2 │    85  female       2
   3 │    98  male         1
   4 │    95  male         2
   5 │    78  male         2
   6 │    89  male         1
```

This is a non-copying operation. One can perform it only if `val` vector has the same length as number
of rows of `df1` or as a special case if `df1` would not have any columns.

```jldoctest dataframe
julia> df1.Age === val # no copy is performed
true
```

If in indexing you select a subset of rows from a data frame the mutation is
performed in place, i.e. writing to an existing vector.
Below setting values of column `:Job` in rows `1:3` to values `[2, 4, 6]`:

```jldoctest dataframe
julia> df1[1:3, :Job] = [2, 3, 2]
3-element Vector{Int64}:
 2
 3
 2

julia> df1
6×3 DataFrame
 Row │ Age    Sex      Job
     │ Int64  String7  Int64
─────┼───────────────────────
   1 │    80  male         2
   2 │    85  female       3
   3 │    98  male         2
   4 │    95  male         2
   5 │    78  male         2
   6 │    89  male         1
```

As a special rule using `!` as row selector replaces column without copying
(just like in the `df1.Age = val` example above).
For example below we replace the `:Sex` column:

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

Similarly to setting selected rows of a single column we can also set
selected columns of a given row of a data frame:

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

We have already mentioned that `DataFrameRow` can be used to mutate its parent data frame.
Here are a few examples:

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

This operations updated the data stored in the `df1` data frame.

In a similar fashion views can be used to update data stored in their parent
data frame. Here are some examples:

```jldoctest dataframe
julia> sdf = view(df1, :, 2:3)
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

In all these cases the parent of `sdf` view was also updated.

### Broadcasting Assignment

Apart from normal assignment one can perform broadcasting assignment using the `.=` operation.

Before we move forward let us explain how broadcasting works in Julia.
The standard syntax to perform [broadcasting](https://docs.julialang.org/en/v1/manual/mathematical-operations/#man-dot-operators)
is to use `.`. For example, as opposed to R this operation fails:

```jldoctest dataframe
julia> s = [25, 26, 35, 56]
4-element Vector{Int64}:
 25
 26
 35
 56

julia> s[2:3] = 0
ERROR: ArgumentError: indexed assignment with a single value to possibly many locations is not supported; perhaps use broadcasting `.=` instead?
```

Instead we have to write:

```jldoctest dataframe
julia> s[2:3] .= 0
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
   1 │    85  male             2
   2 │    89  female           2
   3 │    78  male             4
   4 │    58  transgender      2
   5 │    96  female           2
   6 │    68  female           3
```

Using the `:` instead of `!` above would perform a broadcasting assignment in-place into
an existing column. The major difference between in-place and replace operations
is that replacing columns is needed if new values have a different type than the old ones.

In the examples below we operate on columns `:Customers` and `:City` that are not
present in `df1`. In this case using `!` and `:` are equivalent and a new column
is allocated:

```jldoctest dataframe
julia> df1[!, :Customers] .= ["Rohit", "Akshat", "Rahul", "Aayush", "Prateek", "Anam"]
6-element Vector{String}:
 "Rohit"
 "Akshat"
 "Rahul"
 "Aayush"
 "Prateek"
 "Anam"

julia> df1[:, :City] .= ["Kanpur", "Lucknow", "Bhuvneshwar", "Jaipur", "Ranchi", "Dehradoon"]
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
   1 │    85  male             2  Rohit      Kanpur
   2 │    89  female           2  Akshat     Lucknow
   3 │    78  male             4  Rahul      Bhuvneshwar
   4 │    58  transgender      2  Aayush     Jaipur
   5 │    96  female           2  Prateek    Ranchi
   6 │    68  female           3  Anam       Dehradoon
```

A most common broadcasting assignment operation is when a scalar is used on the
right hand side, e.g:

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

For `:` row selector the broadcasting assignment operation works in-place,
so the following operation throws an error:

```jldoctest dataframe
julia> df1[:, :Age] .= "Economics"
ERROR: MethodError: Cannot `convert` an object of type String to an object of type Int64
```

We need to use `!` instead as it replaces the old vector with a freshly allocated one:

```jldoctest dataframe
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

There are some scenarios in DataFrames.jl, when we naturally want a broadcasting-like behaviour,
but do not allow for the use of `.` operation. In such cases a so-called pseudo-broadcasting
is performed for user convenience. We have already seen it in examples of `DataFrame` constructor.
Below we show pseudo-broadcasting at work in the `insertcols!` function, that inserts
a column into a data frame in an arbitrary position.

In the example below we are creating a column `:Country` with the `insertcols!` function.
Since we pass a scalar `"India"` value of the column it is broadcasted to all rows in the output data frame:

```jldoctest dataframe
julia> insertcols!(df1, 1, :Country => "India")
6×6 DataFrame
 Row │ Country  Age        Sex          Job    Customers  City
     │ String   String     String       Int64  String     String
─────┼────────────────────────────────────────────────────────────────
   1 │ India    Economics  male             4  Rohit      Kanpur
   2 │ India    Economics  female           4  Akshat     Lucknow
   3 │ India    Economics  male             4  Rahul      Bhuvneshwar
   4 │ India    Economics  transgender      4  Aayush     Jaipur
   5 │ India    Economics  female           4  Prateek    Ranchi
   6 │ India    Economics  female           4  Anam       Dehradoon
```

You can pass a column location where you want to put the inserted column as a
second argument to the `insertcols!` function:
```
julia> insertcols!(df1, 4, :b => exp(4))
6×7 DataFrame
 Row │ Country  Age        Sex          b        Job    Customers  City        ⋯
     │ String   String     String       Float64  Int64  String     String      ⋯
─────┼──────────────────────────────────────────────────────────────────────────
   1 │ India    Economics  male         54.5982      4  Rohit      Kanpur      ⋯
   2 │ India    Economics  female       54.5982      4  Akshat     Lucknow
   3 │ India    Economics  male         54.5982      4  Rahul      Bhuvneshwar
   4 │ India    Economics  transgender  54.5982      4  Aayush     Jaipur
   5 │ India    Economics  female       54.5982      4  Prateek    Ranchi      ⋯
   6 │ India    Economics  female       54.5982      4  Anam       Dehradoon
```

### Not, Between, Cols, and All Column Selectors

You can use `Not`, `Between`, `Cols`, and `All` selectors in more complex column selection
scenarios:
- `Not` selector (from the [InvertedIndices.jl](https://github.com/mbauman/InvertedIndices.jl) package)
  allows us to specify the columns we want to exclude from the resulting data frame.
  We can put any valid other column selector inside `Not`;
- `Between` selector allows us to specify a range of columns (we can pass the start and stop column using any
  of the single column selector syntaxes);
- `Cols(...)` selector picks a union of other selectors passed as its arguments;
- `All()` allows us to select all columns of `DataFrame`; this is the same as passing `:`;
- regular expression to select columns whose names match it.

Let us give some examples of these selectors.

Drop `:Age` column:

```jldoctest dataframe
julia> german[:, Not(:Age)]
1000×9 DataFrame
  Row │ id     Sex      Job    Housing  Saving accounts  Checking account  Cre ⋯
      │ Int64  String7  Int64  String7  String15         String15          Int ⋯
──────┼─────────────────────────────────────────────────────────────────────────
    1 │     0  male         2  own      NA               little                ⋯
    2 │     1  female       2  own      little           moderate
    3 │     2  male         1  own      little           NA
    4 │     3  male         2  free     little           little
    5 │     4  male         2  free     little           little                ⋯
    6 │     5  male         1  free     NA               NA
    7 │     6  male         2  own      quite rich       NA
    8 │     7  male         3  rent     little           moderate
  ⋮   │   ⋮       ⋮       ⋮       ⋮            ⋮                ⋮              ⋱
  994 │   993  male         3  own      little           little                ⋯
  995 │   994  male         2  own      NA               NA
  996 │   995  female       1  own      little           NA
  997 │   996  male         3  own      little           little
  998 │   997  male         2  own      little           NA                    ⋯
  999 │   998  male         2  free     little           little
 1000 │   999  male         2  own      moderate         moderate
                                                  3 columns and 985 rows omitted
```

Select columns starting from `:Sex` and ending at `:Housing`:

```
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
```

In the example below `Cols` selector is picking a union of `"Age"` and `Between("Sex", "Job")`
selectors passed as its arguments:

```jldoctest dataframe
julia> german[:, Cols("Age", Between("Sex", "Job"))]
1000×3 DataFrame
  Row │ Age    Sex      Job
      │ Int64  String7  Int64
──────┼───────────────────────
    1 │    67  male         2
    2 │    22  female       2
    3 │    49  male         1
    4 │    45  male         2
    5 │    53  male         2
    6 │    35  male         1
    7 │    53  male         2
    8 │    35  male         3
  ⋮   │   ⋮       ⋮       ⋮
  994 │    30  male         3
  995 │    50  male         2
  996 │    31  female       1
  997 │    40  male         3
  998 │    38  male         2
  999 │    23  male         2
 1000 │    27  male         2
              985 rows omitted
```

You can also use `Regex` (regular expressions) to select columns. In the example
below we select columns that have `"S"` in their name and also we use `Not` to drop row number 5:

```jldoctest dataframe
julia> german[Not(5), r"S"]
999×2 DataFrame
 Row │ Sex      Saving accounts
     │ String7  String15
─────┼──────────────────────────
   1 │ male     NA
   2 │ female   little
   3 │ male     little
   4 │ male     little
   5 │ male     NA
   6 │ male     quite rich
   7 │ male     little
   8 │ male     rich
  ⋮  │    ⋮            ⋮
 993 │ male     little
 994 │ male     NA
 995 │ female   little
 996 │ male     little
 997 │ male     little
 998 │ male     little
 999 │ male     moderate
                984 rows omitted
```

## Basic Usage of Transformation Functions

In DataFrames.jl we have five functions that we can be used to perform transformations of columns
of a data frame:

- `combine`: creates a new data frame populated with columns that are results of transformation
  applied to the source data frame columns, potentially combining its rows;
- `select`: creates a new data frame that has the same number of rows as the source data frame
  populated with columns that are results of transformations applied to the source data frame
  columns;
- `select!`: the same as `select` but updates the passed data frame in place;
- `transform`: the same as `select` but keeps the columns that were already present in the data frame
  (note though that these columns can be potentially modified by the transformation passed to `transform`);
- `transform!`: the same as `transform` but updates the passed data frame in place.

The fundamental ways to specify a transformation are:

- `source_column => transformation => target_column_name`;
  In this scenario the `source_column` is passed as an argument to `transformation` function
  and stored in `target_column_name` column.
- `source_column => transformation`;
  In this scenario we apply the transformation function to `source_column` and the
  target column names is automatically generated.
- `source_column => target_column_name` renames the `source_column` to `target_column_name`.
- `source_column` just keep the source column as is in the result without any transformation;

These rules are typically called transformation mini-language.

Let us move to the examples of application of these rules

```jldoctest dataframe
julia> using Statistics

julia> combine(german, :Age => mean => :mean_age)
1×1 DataFrame
 Row │ mean_age
     │ Float64
─────┼──────────
   1 │   35.546

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
```

As you can see in both cases the `mean` function was applied to `:Age` column
and the result was stored in the `:mean_age` column. The difference between
the `combine` and `select` functions is that the `combine` aggregates data
and produces as many rows as were returned by the transformation function.
On the other hand the `select` function always keeps the number of rows in a
data frame to be the same as in the source data frame. Therefore in this case
the result of the `mean` function got broadcasted.

As `combine` potentially allows any number of rows to be produced as a result
of the transformation if we have a combination of transformations where some of
them produce a vector, and other produce scalars then scalars get broadcasted
exactly like in  `select`. Here is an example:

```jldoctest dataframe
julia> combine(german, :Age => mean => :mean_age, :Housing => unique => :housing)
3×2 DataFrame
 Row │ mean_age  housing
     │ Float64   String7
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
```

This pattern is encountered very often in practice, therefore there is a `ByRow` convenience wrapper for a
function that creates its broadcasted variant. In these examples `ByRow` is a special type used for
selection operations to signal that the wrapped function should be applied to each element (row) of the selection.
Here we are passing `ByRow` wrapper to target column name `:Sex` using `uppercase` function:

```jldoctest dataframe
julia> select(german, :Sex => ByRow(uppercase) => :SEX)
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
```

In this case we transform our source column `:Age` using `ByRow` wrapper and
automatically generate the target column name:

```jldoctest dataframe
julia> select(german, :Age, :Age => ByRow(sqrt))
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

When we pass just a column (without the `=>` part) we can use any column selector
that is allowed in indexing. Here we exclude the column `:Age` from
the resulting data frame:

```jldoctest dataframe
julia> select(german, Not(:Age))
1000×9 DataFrame
  Row │ id     Sex      Job    Housing  Saving accounts  Checking account  Cre ⋯
      │ Int64  String7  Int64  String7  String15         String15          Int ⋯
──────┼─────────────────────────────────────────────────────────────────────────
    1 │     0  male         2  own      NA               little                ⋯
    2 │     1  female       2  own      little           moderate
    3 │     2  male         1  own      little           NA
    4 │     3  male         2  free     little           little
    5 │     4  male         2  free     little           little                ⋯
    6 │     5  male         1  free     NA               NA
    7 │     6  male         2  own      quite rich       NA
    8 │     7  male         3  rent     little           moderate
  ⋮   │   ⋮       ⋮       ⋮       ⋮            ⋮                ⋮              ⋱
  994 │   993  male         3  own      little           little                ⋯
  995 │   994  male         2  own      NA               NA
  996 │   995  female       1  own      little           NA
  997 │   996  male         3  own      little           little
  998 │   997  male         2  own      little           NA                    ⋯
  999 │   998  male         2  free     little           little
 1000 │   999  male         2  own      moderate         moderate
                                                  3 columns and 985 rows omitted
```

In order to select a column we just passed them as argument. As another example
let us present that the `r"S"` regular expression we used above also works
as we have described above:

```jldoctest dataframe
julia> select(german, r"S")
1000×2 DataFrame
  Row │ Sex      Saving accounts
      │ String7  String15
──────┼──────────────────────────
    1 │ male     NA
    2 │ female   little
    3 │ male     little
    4 │ male     little
    5 │ male     little
    6 │ male     NA
    7 │ male     quite rich
    8 │ male     little
  ⋮   │    ⋮            ⋮
  994 │ male     little
  995 │ male     NA
  996 │ female   little
  997 │ male     little
  998 │ male     little
  999 │ male     little
 1000 │ male     moderate
                 985 rows omitted
```

The benefit of `select` or `combine` over indexing is that it is easier
to combine several column selectors, e.g.:

```jldoctest dataframe
julia> select(german, r"S", "Job", 1)
1000×4 DataFrame
  Row │ Sex      Saving accounts  Job    id
      │ String7  String15         Int64  Int64
──────┼────────────────────────────────────────
    1 │ male     NA                   2      0
    2 │ female   little               2      1
    3 │ male     little               1      2
    4 │ male     little               2      3
    5 │ male     little               2      4
    6 │ male     NA                   1      5
    7 │ male     quite rich           2      6
    8 │ male     little               3      7
  ⋮   │    ⋮            ⋮           ⋮      ⋮
  994 │ male     little               3    993
  995 │ male     NA                   2    994
  996 │ female   little               1    995
  997 │ male     little               3    996
  998 │ male     little               2    997
  999 │ male     little               2    998
 1000 │ male     moderate             2    999
                               985 rows omitted
```

Taking advantage of this flexibility here is an idiomatic pattern to move some column to the front of a data frame:

```jldoctest dataframe
julia> select(german, "Sex", :)
1000×10 DataFrame
  Row │ Sex      id     Age    Job    Housing  Saving accounts  Checking accou ⋯
      │ String7  Int64  Int64  Int64  String7  String15         String15       ⋯
──────┼─────────────────────────────────────────────────────────────────────────
    1 │ male         0     67      2  own      NA               little         ⋯
    2 │ female       1     22      2  own      little           moderate
    3 │ male         2     49      1  own      little           NA
    4 │ male         3     45      2  free     little           little
    5 │ male         4     53      2  free     little           little         ⋯
    6 │ male         5     35      1  free     NA               NA
    7 │ male         6     53      2  own      quite rich       NA
    8 │ male         7     35      3  rent     little           moderate
  ⋮   │    ⋮       ⋮      ⋮      ⋮       ⋮            ⋮                ⋮       ⋱
  994 │ male       993     30      3  own      little           little         ⋯
  995 │ male       994     50      2  own      NA               NA
  996 │ female     995     31      1  own      little           NA
  997 │ male       996     40      3  own      little           little
  998 │ male       997     38      2  own      little           NA             ⋯
  999 │ male       998     23      2  free     little           little
 1000 │ male       999     27      2  own      moderate         moderate
                                                  4 columns and 985 rows omitted
```

Below, we are simply passing source column and target column name to rename them
(without specifying the transformation part):

```jldoctest dataframe
julia> select(german, :Sex => :x1, :Age => :x2)
1000×2 DataFrame
  Row │ x1       x2
      │ String7  Int64
──────┼────────────────
    1 │ male        67
    2 │ female      22
    3 │ male        49
    4 │ male        45
    5 │ male        53
    6 │ male        35
    7 │ male        53
    8 │ male        35
  ⋮   │    ⋮       ⋮
  994 │ male        30
  995 │ male        50
  996 │ female      31
  997 │ male        40
  998 │ male        38
  999 │ male        23
 1000 │ male        27
       985 rows omitted
```

It is important to note that `select` always returns a data frame, even if a single column selected
as opposed to indexing syntax. Compare the following:

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

By default `select` copies columns of a passed source data frame. In order to avoid copying, pass
the `copycols=false` keyword argument:

```jldoctest dataframe
julia> df = select(german, :Sex)
1000×1 DataFrame
  Row │ Sex
      │ String7
──────┼─────────
    1 │ male
    2 │ female
    3 │ male
    4 │ male
    5 │ male
    6 │ male
    7 │ male
    8 │ male
  ⋮   │    ⋮
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
      │ String7
──────┼─────────
    1 │ male
    2 │ female
    3 │ male
    4 │ male
    5 │ male
    6 │ male
    7 │ male
    8 │ male
  ⋮   │    ⋮
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
julia> select!(german, Not(:Age));

julia> german
1000×9 DataFrame
  Row │ id     Sex      Job    Housing  Saving accounts  Checking account  Cre ⋯
      │ Int64  String7  Int64  String7  String15         String15          Int ⋯
──────┼─────────────────────────────────────────────────────────────────────────
    1 │     0  male         2  own      NA               little                ⋯
    2 │     1  female       2  own      little           moderate
    3 │     2  male         1  own      little           NA
    4 │     3  male         2  free     little           little
    5 │     4  male         2  free     little           little                ⋯
    6 │     5  male         1  free     NA               NA
    7 │     6  male         2  own      quite rich       NA
    8 │     7  male         3  rent     little           moderate
  ⋮   │   ⋮       ⋮       ⋮       ⋮            ⋮                ⋮              ⋱
  994 │   993  male         3  own      little           little                ⋯
  995 │   994  male         2  own      NA               NA
  996 │   995  female       1  own      little           NA
  997 │   996  male         3  own      little           little
  998 │   997  male         2  own      little           NA                    ⋯
  999 │   998  male         2  free     little           little
 1000 │   999  male         2  own      moderate         moderate
                                                  3 columns and 985 rows omitted
```

As you can see the `:Age` column was dropped from the `german` data frame.

The `transform` and `transform!` functions work identically to `select` and `select!` with the only difference that
they retain all columns that are present in the source data frame. Here are some examples:

```jldoctest dataframe
julia> german = copy(german_ref);

julia> df = german_ref[1:8, 1:5]
8×5 DataFrame
 Row │ id     Age    Sex      Job    Housing
     │ Int64  Int64  String7  Int64  String7
─────┼───────────────────────────────────────
   1 │     0     67  male         2  own
   2 │     1     22  female       2  own
   3 │     2     49  male         1  own
   4 │     3     45  male         2  free
   5 │     4     53  male         2  free
   6 │     5     35  male         1  free
   7 │     6     53  male         2  own
   8 │     7     35  male         3  rent

julia> transform(df, :Age => maximum)
8×6 DataFrame
 Row │ id     Age    Sex      Job    Housing  Age_maximum
     │ Int64  Int64  String7  Int64  String7  Int64
─────┼────────────────────────────────────────────────────
   1 │     0     67  male         2  own               67
   2 │     1     22  female       2  own               67
   3 │     2     49  male         1  own               67
   4 │     3     45  male         2  free              67
   5 │     4     53  male         2  free              67
   6 │     5     35  male         1  free              67
   7 │     6     53  male         2  own               67
   8 │     7     35  male         3  rent              67
```

In the example below we are swapping values stored in columns `:Sex` and `:Age`:

```jldoctest dataframe
julia> transform(german, :Age => :Sex, :Sex => :Age)
1000×10 DataFrame
  Row │ id     Age      Sex    Job    Housing  Saving accounts  Checking accou ⋯
      │ Int64  String7  Int64  Int64  String7  String15         String15       ⋯
──────┼─────────────────────────────────────────────────────────────────────────
    1 │     0  male        67      2  own      NA               little         ⋯
    2 │     1  female      22      2  own      little           moderate
    3 │     2  male        49      1  own      little           NA
    4 │     3  male        45      2  free     little           little
    5 │     4  male        53      2  free     little           little         ⋯
    6 │     5  male        35      1  free     NA               NA
    7 │     6  male        53      2  own      quite rich       NA
    8 │     7  male        35      3  rent     little           moderate
  ⋮   │   ⋮       ⋮       ⋮      ⋮       ⋮            ⋮                ⋮       ⋱
  994 │   993  male        30      3  own      little           little         ⋯
  995 │   994  male        50      2  own      NA               NA
  996 │   995  female      31      1  own      little           NA
  997 │   996  male        40      3  own      little           little
  998 │   997  male        38      2  own      little           NA             ⋯
  999 │   998  male        23      2  free     little           little
 1000 │   999  male        27      2  own      moderate         moderate
                                                  4 columns and 985 rows omitted
```

If we give more than one source column to a transformation they are passed as
consecutive positional arguments.
So for example the `[:Age, :Job] => (+) => :res` transformation below
evaluates `+(df1.Age, df1.Job)` (which adds two columns)
and stores the result in the `:res` column:

```jldoctest dataframe
julia> select(german, :Age, :Job, [:Age, :Job] => (+) => :res)
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

In the examples given in this introductory tutorial we did not cover all
options of the transformation mini-language. More advanced examples, in particular
showing how to pass or produce multiple columns using the `AsTable` operation
(which you might have seen in some DataFrames.jl demos) are given in the later
sections of the manual.
