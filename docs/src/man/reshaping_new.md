# Reshaping Data

In `DataFrames.jl` currently we have two functions: `stack`, and `unstack`
that we can use for reshaping our data. Their goals are very simple:
- `stack` allows us to go from wide to long data format;
- `unstack` allows us to go from long to wide data format. 

In wide data it has a column for each variable whereas long format data has 
a column for possible variable types & a column for the values of those variables.

Reshape data from wide to long format using the stack function:

```jldoctest dataframe
julia> using DataFrames

julia> using CSV

julia> german_ref = CSV.read(joinpath(dirname(pathof(DataFrames)),
                                       "..", "docs", "src", "assets", "german.csv"),
                              DataFrame; missingstring="NA")
1000×10 DataFrame
  Row │ id     Age    Sex     Job    Housing  Saving accounts  Checking accoun ⋯
      │ Int64  Int64  String  Int64  String   String?          String?         ⋯
──────┼─────────────────────────────────────────────────────────────────────────
    1 │     0     67  male        2  own      missing          little          ⋯
    2 │     1     22  female      2  own      little           moderate
    3 │     2     49  male        1  own      little           missing
    4 │     3     45  male        2  free     little           little
    5 │     4     53  male        2  free     little           little          ⋯
    6 │     5     35  male        1  free     missing          missing
    7 │     6     53  male        2  own      quite rich       missing
    8 │     7     35  male        3  rent     little           moderate
  ⋮   │   ⋮      ⋮      ⋮       ⋮       ⋮            ⋮                ⋮        ⋱
  994 │   993     30  male        3  own      little           little          ⋯
  995 │   994     50  male        2  own      missing          missing
  996 │   995     31  female      1  own      little           missing
  997 │   996     40  male        3  own      little           little
  998 │   997     38  male        2  own      little           missing         ⋯
  999 │   998     23  male        2  free     little           little
 1000 │   999     27  male        2  own      moderate         moderate
                                                  4 columns and 985 rows omitted

julia> german = view(german_ref, 1:10, 1:6)
10×6 SubDataFrame
 Row │ id     Age    Sex     Job    Housing  Saving accounts
     │ Int64  Int64  String  Int64  String   String?
─────┼───────────────────────────────────────────────────────
   1 │     0     67  male        2  own      missing
   2 │     1     22  female      2  own      little
   3 │     2     49  male        1  own      little
   4 │     3     45  male        2  free     little
   5 │     4     53  male        2  free     little
   6 │     5     35  male        1  free     missing
   7 │     6     53  male        2  own      quite rich
   8 │     7     35  male        3  rent     little
   9 │     8     61  male        1  own      rich
  10 │     9     28  male        3  own      little                                                  
```

## The basics of `stack`

Stack return the long-format `DataFrame` with columns for each of the ID variables, column 
value name holding the values of the stacked columns, and column variable name a vector holding 
the name of the corresponding measurement variables.

Here, `german` is the `AbstractDataFrame` which has to be stacked and the columns to
be stacked are `:Age`, `:Job`, `:Housing`, `:Sex` which is known as the measurement variables;
`:variable` is the name of our new stacked columns which are holding the names of each of the
measurement variables, and `value` is the name of the new stacked columns containing the values
from each of the measurement variables:

```jldoctest dataframe
julia> stack(german, [:Age, :Job, :Housing])
30×5 DataFrame
 Row │ id     Sex     Saving accounts  variable  value
     │ Int64  String  String?          String    Any
─────┼─────────────────────────────────────────────────
   1 │     0  male    missing          Age       67
   2 │     1  female  little           Age       22
   3 │     2  male    little           Age       49
   4 │     3  male    little           Age       45
   5 │     4  male    little           Age       53
   6 │     5  male    missing          Age       35
   7 │     6  male    quite rich       Age       53
   8 │     7  male    little           Age       35
  ⋮  │   ⋮      ⋮            ⋮            ⋮        ⋮
  24 │     3  male    little           Housing   free
  25 │     4  male    little           Housing   free
  26 │     5  male    missing          Housing   free
  27 │     6  male    quite rich       Housing   own
  28 │     7  male    little           Housing   rent
  29 │     8  male    rich             Housing   own
  30 │     9  male    little           Housing   own
                                        15 rows omitted

julia> stack(german, [:Age, :Job, :Housing], [:Sex])
30×3 DataFrame
 Row │ Sex     variable  value
     │ String  String    Any
─────┼─────────────────────────
   1 │ male    Age       67
   2 │ female  Age       22
   3 │ male    Age       49
   4 │ male    Age       45
   5 │ male    Age       53
   6 │ male    Age       35
   7 │ male    Age       53
   8 │ male    Age       35
  ⋮  │   ⋮        ⋮        ⋮
  24 │ male    Housing   free
  25 │ male    Housing   free
  26 │ male    Housing   free
  27 │ male    Housing   own
  28 │ male    Housing   rent
  29 │ male    Housing   own
  30 │ male    Housing   own
                15 rows omitted
```

Below we have omitted second argument in `stack`, so all other columns are assumed to be the
[id_variables](https://github.com/JuliaData/DataFrames.jl/blob/f690aa49e958f51e0c3c579b6def1f11be214d98/src/abstractdataframe/reshape.jl#:~:text=id_vars%60%20%3A%20the%20identifier,are%20not%20%60measure_vars%60):

```jldoctest dataframe
julia> stack(german, Not([:Sex, :id]))
40×4 DataFrame
 Row │ id     Sex     variable         value
     │ Int64  String  String           Any
─────┼────────────────────────────────────────────
   1 │     0  male    Age              67
   2 │     1  female  Age              22
   3 │     2  male    Age              49
   4 │     3  male    Age              45
   5 │     4  male    Age              53
   6 │     5  male    Age              35
   7 │     6  male    Age              53
   8 │     7  male    Age              35
  ⋮  │   ⋮      ⋮            ⋮             ⋮
  34 │     3  male    Saving accounts  little
  35 │     4  male    Saving accounts  little
  36 │     5  male    Saving accounts  missing
  37 │     6  male    Saving accounts  quite rich
  38 │     7  male    Saving accounts  little
  39 │     8  male    Saving accounts  rich
  40 │     9  male    Saving accounts  little
                                   25 rows omitted
```

We can rename columns:

```jldoctest dataframe
julia> stack(german, Not([:Sex, :id]), variable_name=:x, value_name=:y)
40×4 DataFrame
 Row │ id     Sex     x                y
     │ Int64  String  String           Any
─────┼────────────────────────────────────────────
   1 │     0  male    Age              67
   2 │     1  female  Age              22
   3 │     2  male    Age              49
   4 │     3  male    Age              45
   5 │     4  male    Age              53
   6 │     5  male    Age              35
   7 │     6  male    Age              53
   8 │     7  male    Age              35
  ⋮  │   ⋮      ⋮            ⋮             ⋮
  34 │     3  male    Saving accounts  little
  35 │     4  male    Saving accounts  little
  36 │     5  male    Saving accounts  missing
  37 │     6  male    Saving accounts  quite rich
  38 │     7  male    Saving accounts  little
  39 │     8  male    Saving accounts  rich
  40 │     9  male    Saving accounts  little
                                   25 rows omitted
```                                           

## The basics of `unstack`

*Unstack* converts the data frame `german` from it long to wide format. Row and column 
keys will be ordered in the order of their first appearance.    

```jldoctest dataframe
julia> long = stack(german, [:Age, :Sex, :Job, :Housing])
40×4 DataFrame
 Row │ id     Saving accounts  variable  value
     │ Int64  String?          String    Any
─────┼─────────────────────────────────────────
   1 │     0  missing          Age       67
   2 │     1  little           Age       22
   3 │     2  little           Age       49
   4 │     3  little           Age       45
   5 │     4  little           Age       53
   6 │     5  missing          Age       35
   7 │     6  quite rich       Age       53
   8 │     7  little           Age       35
  ⋮  │   ⋮           ⋮            ⋮        ⋮
  34 │     3  little           Housing   free
  35 │     4  little           Housing   free
  36 │     5  missing          Housing   free
  37 │     6  quite rich       Housing   own
  38 │     7  little           Housing   rent
  39 │     8  rich             Housing   own
  40 │     9  little           Housing   own
                                25 rows omitted
```

We can even skip passing the `:variable` and `:value` values as positional arguments, as they 
will be used by default. In this case all columns other than named `:variable` and `:value` 
are treated as keys:

```jldoctest dataframe
julia> unstack(long)
10×6 DataFrame
 Row │ id     Saving accounts  Age  Sex     Job  Housing
     │ Int64  String?          Any  Any     Any  Any
─────┼───────────────────────────────────────────────────
   1 │     0  missing          67   male    2    own
   2 │     1  little           22   female  2    own
   3 │     2  little           49   male    1    own
   4 │     3  little           45   male    2    free
   5 │     4  little           53   male    2    free
   6 │     5  missing          35   male    1    free
   7 │     6  quite rich       53   male    2    own
   8 │     7  little           35   male    3    rent
   9 │     8  rich             61   male    1    own
  10 │     9  little           28   male    3    own
```

If the remaining columns are unique, we can skip the id variable and use:

```jldoctest dataframe
julia> unstack(long, :variable, :value)
10×6 DataFrame
 Row │ id     Saving accounts  Age  Sex     Job  Housing
     │ Int64  String?          Any  Any     Any  Any
─────┼───────────────────────────────────────────────────
   1 │     0  missing          67   male    2    own
   2 │     1  little           22   female  2    own
   3 │     2  little           49   male    1    own
   4 │     3  little           45   male    2    free
   5 │     4  little           53   male    2    free
   6 │     5  missing          35   male    1    free
   7 │     6  quite rich       53   male    2    own
   8 │     7  little           35   male    3    rent
   9 │     8  rich             61   male    1    own
  10 │     9  little           28   male    3    own
```

Below all other columns are treated as keys:

```jldoctest dataframe
julia> unstack(long, :id, :variable, :value)
10×5 DataFrame
 Row │ id     Age  Sex     Job  Housing
     │ Int64  Any  Any     Any  Any
─────┼──────────────────────────────────
   1 │     0  67   male    2    own
   2 │     1  22   female  2    own
   3 │     2  49   male    1    own
   4 │     3  45   male    2    free
   5 │     4  53   male    2    free
   6 │     5  35   male    1    free
   7 │     6  53   male    2    own
   8 │     7  35   male    3    rent
   9 │     8  61   male    1    own
  10 │     9  28   male    3    own

julia> unstack(long, [:1, :2], :variable, :value)
10×6 DataFrame
 Row │ id     Saving accounts  Age  Sex     Job  Housing
     │ Int64  String?          Any  Any     Any  Any
─────┼───────────────────────────────────────────────────
   1 │     0  missing          67   male    2    own
   2 │     1  little           22   female  2    own
   3 │     2  little           49   male    1    own
   4 │     3  little           45   male    2    free
   5 │     4  little           53   male    2    free
   6 │     5  missing          35   male    1    free
   7 │     6  quite rich       53   male    2    own
   8 │     7  little           35   male    3    rent
   9 │     8  rich             61   male    1    own
  10 │     9  little           28   male    3    own
```

We can rename the unstacked columns:

```jldoctest dataframe
julia> unstack(long, :id, :variable, :value, renamecols=x->Symbol(:_, x))
10×5 DataFrame
 Row │ id     _Age  _Sex    _Job  _Housing
     │ Int64  Any   Any     Any   Any
─────┼─────────────────────────────────────
   1 │     0  67    male    2     own
   2 │     1  22    female  2     own
   3 │     2  49    male    1     own
   4 │     3  45    male    2     free
   5 │     4  53    male    2     free
   6 │     5  35    male    1     free
   7 │     6  53    male    2     own
   8 │     7  35    male    3     rent
   9 │     8  61    male    1     own
  10 │     9  28    male    3     own
```

### Pivot tables with `unstack`

Most likely we want to aggregate grocessary per year using the `sum` function. This is a classic pivot 
table task. In DataFrames.jl currently one does it in two steps: first aggregate, then reshape. Here 
is how we can do it (We are showing two separate steps, but you could use e.g. [Chain.jl](https://github.com/jkrumbiegel/Chain.jl) to streamline the processing):

```jldoctest dataframe
julia> df = combine(groupby(german, [:Age, :Sex]), :Job => sum => :Job)
8×3 DataFrame
 Row │ Age    Sex     Job
     │ Int64  String  Int64
─────┼──────────────────────
   1 │    67  male        2
   2 │    22  female      2
   3 │    49  male        1
   4 │    45  male        2
   5 │    53  male        4
   6 │    35  male        4
   7 │    61  male        1
   8 │    28  male        3

julia> unstack(df, :Age, :Sex, :Job)
8×3 DataFrame
 Row │ Age    male     female
     │ Int64  Int64?   Int64?
─────┼─────────────────────────
   1 │    67        2  missing
   2 │    22  missing        2
   3 │    49        1  missing
   4 │    45        2  missing
   5 │    53        4  missing
   6 │    35        4  missing
   7 │    61        1  missing
   8 │    28        3  missing
```
