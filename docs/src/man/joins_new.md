# Joining DataFrames

"You may have two different data sets in Julia but you may not be sure how to perform *joins*. What 
criteria should I consider? What are the different ways I can join these data sets?"

!!! note

  Some of the part has been taken from [Bence Komarniczky](https://towardsdatascience.com/joining-dataframes-in-julia-c435e3da32f3)

Sound familiar? You may have come across this question plenty of times on online discussion
forums. Working with one dataframe is fairly straightforward but things become challenging when
we have data spread across two or more data frames. This is where the concept of *joins* come in.

*Joins* is a very common and important operation that arises in the world of tabulated data. 
A join across two data frames is the action of combining the two datasets based on shared column values 
that exist across them. We call this column (or columns) the *key(s)*. So, each record from the 
first data frame is matched to a record in the second data frame — as long as the records’ key values are 
the same. 

Previously, we have used German Credit Card dataset. Now let's say you are working
on a classification problem but we do not have target column in our dataset. We are given two data frames – 
one which contains data about customer and the other that has taget (risk factor) information. We will 
use these data frames  to understand how the different types of *joins* work using `DataFrames.jl`.

For example, suppose that we have the following two data frames:

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

This table has all the `:id`s of all factors to make comparisions for risk factor. Let’s say we 
have another data frame, which has the risk (good or bad) of these individuals:

```jldoctest dataframe
julia> risk_ref = CSV.read(joinpath(dirname(pathof(DataFrames)),
                                    "..", "docs", "src", "assets", "risk.csv"),
                           DataFrame)
1000×2 DataFrame
  Row │ id     Risk
      │ Int64  String
──────┼───────────────
    1 │     0  good
    2 │     1  bad
    3 │     2  good
    4 │     3  good
    5 │     4  bad
    6 │     5  good
    7 │     6  good
    8 │     7  good
  ⋮   │   ⋮      ⋮
  994 │   993  good
  995 │   994  good
  996 │   995  good
  997 │   996  good
  998 │   997  good
  999 │   998  bad
 1000 │   999  good
      985 rows omitted
```

We now have 2 data frames:
- `german_ref` holds the `:id`s, `:Age`, `:Sex`, `:Job` and so on
- `risk_ref` holds the `:id`s and `:Risk`.

We would like to combine the 2 data frames so that we can see columns of `german_ref` and `risk_ref` together. 
We do a `innerjoin`!

Rules for the `on` keyword argument are as follows:
- a single `Symbol` or string if joining on one column with the same name, e.g. `on=:id`
- a `Pair` of `Symbol`s or strings if joining on one column with different names, e.g. `on=:id=>:id2`
- a `vector` of `Symbol`s or strings if joining on multiple columns with the same name, 
  e.g. `on=[:id1, :id2]`
- a `vector` of `Pair`s of `Symbol`s or strings if joining on multiple columns with the same name, 
  e.g. `on=[:a1=>:a2, :b1=>:b2]`
- a `vector` containing a combination of `Symbol`s or strings or `Pair` of Symbols or strings, 
  e.g. `on=[:a1=>:a2, :b1]`

Here, we are working with a average (in size) data set that contains both the `:Age` and `:Sex` for each `:id`. 
We can do this using the `innerjoin` function:

```jldoctest dataframe
julia> innerjoin(german_ref, risk_ref, on = :id)
1000×11 DataFrame
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
                                                  5 columns and 985 rows omitted
```

Let’s go through this in detail. Arguments 1 and 2 `german_ref` & `risk_ref` are the two tables we’re 
joining. Argument 3 `on` tells us what the key column is. We’ll use this column to match the observations 
across the data frames. As you can see, we ended up with *1000 rows* and *11 columns*. Now go back to the 
beginning and check out how the 2 original data frames looked like. Make sure you understand why we ended up 
with these rows only. Since we’re using `innerjoin`, we’re only keeping `:id`s that appear in both data frames — 
everything else we throw away.

In relational database theory, the above operation is generally referred to as a join. The columns used to 
determine which rows should be combined during a join are called keys.

The following functions are provided to perform seven kinds of joins:

- `innerjoin`: the output contains rows for values of the key that exist in all passed data frames.
- `leftjoin`: the output contains rows for values of the key that exist in the first (left) argument, 
  whether or not that value exists in the second (right) argument.
- `rightjoin`: the output contains rows for values of the key that exist in the second (right) argument, 
  whether or not that value exists in the first (left) argument.
- `outerjoin`: the output contains rows for values of the key that exist in any of the passed data frames.
- `semijoin`: Like an inner join, but output is restricted to columns from the first (left) argument.
- `antijoin`: The output contains rows for values of the key that exist in the first (left) but not the 
   second (right) argument. As with semijoin, output is restricted to columns from the first (left) argument.
- `crossjoin`: The output is the cartesian product of rows from all passed data frames.

We will now go through all of these  joins one-by-one! First, let’s do a `leftjoin` and see what happens to 
`:id`s that are only in the first data frame:

```jldoctest dataframe
julia> persons = DataFrame(id = 1:5, name = ["Rohit", "Mohit", "Rahul", "Vijay", "Akshat"])
5×2 DataFrame
 Row │ id     name
     │ Int64  String
─────┼───────────────
   1 │     1  Rohit
   2 │     2  Mohit
   3 │     3  Rahul
   4 │     4  Vijay
   5 │     5  Akshat
```

This data frame has all the `:id`s of individuals and their names. Let’s say we have another dataframe, 
which has the earnings of these individuals:

```jldoctest dataframe
julia> earnings = DataFrame(id = 3:8, salary = [5000, 1000, 5000, 8000, 3000, 9000])
6×2 DataFrame
 Row │ id     salary
     │ Int64  Int64
─────┼───────────────
   1 │     3    5000
   2 │     4    1000
   3 │     5    5000
   4 │     6    8000
   5 │     7    3000
   6 │     8    9000
```

Now, lets perform `leftjoin` operation:

```jldoctest dataframe
julia> leftjoin(persons, earnings, on = :id)
5×3 DataFrame
 Row │ id     name    salary
     │ Int64  String  Int64?
─────┼────────────────────────
   1 │     3  Rahul      5000
   2 │     4  Vijay      1000
   3 │     5  Akshat     5000
   4 │     1  Rohit   missing
   5 │     2  Mohit   missing
```

Here we kept all of the observations from data frame `persons` no matter what is going on in data frame `earnings`. 
For records without a match in data frame `persons`, the salary column has `missingas` value. This makes sense 
as we never actually saw those earning figures. Of course, there is also a `rightjoin`. This keeps all rows 
from the second data frame.

```jldoctest dataframe
julia> rightjoin(persons, earnings, on = :id)
6×3 DataFrame
 Row │ id     name     salary
     │ Int64  String?  Int64
─────┼────────────────────────
   1 │     3  Rahul      5000
   2 │     4  Vijay      1000
   3 │     5  Akshat     5000
   4 │     6  missing    8000
   5 │     7  missing    3000
   6 │     8  missing    9000
```

If we want to have all `:id`s from both data frames, we use an `outerjoin`:

```jldoctest dataframe
julia> outerjoin(persons, earnings, on = :id)
8×3 DataFrame
 Row │ id     name     salary
     │ Int64  String?  Int64?
─────┼─────────────────────────
   1 │     3  Rahul       5000
   2 │     4  Vijay       1000
   3 │     5  Akshat      5000
   4 │     1  Rohit    missing
   5 │     2  Mohit    missing
   6 │     6  missing     8000
   7 │     7  missing     3000
   8 │     8  missing     9000
```

It loads more `missing` values, but we can see all the `:id`s now. It check out the new `missing` 
names for `:id`s 6–8. It is known as `outerjoin`.

Finally, we can keep only rows that *don't* match with `antijoin`:

```jldoctest dataframe
julia> antijoin(persons, earnings, on = :id)
2×2 DataFrame
 Row │ id     name
     │ Int64  String
─────┼───────────────
   1 │     1  Rohit
   2 │     2  Mohit
```

The above four joins make up the basics of data frame merging. We can remember like this:
- `innerjoin`: The output contains rows for values of the key that exist in both the first (left) and 
  second (right) arguments to `join`.
- `leftjoin`: The output contains rows for values of the key that exist in the first (left) argument to 
  `join`, whether or not that value exists in the second (right) argument.
- `rightjoin`: The output contains rows for values of the key that exist in the second (right) argument 
  to `join`, whether or not that value exists in the first (left) argument.
- `outerjoin`: The output contains rows for values of the key that exist in the first (left) or second 
  (right) argument to `join`.

Now, let’s say we want to look at people’s names for whom we have earnings data, but we don’t actually 
want to have all the columns from the second table. That’s what `semijoin` does. It gives us the same 
rows as an `innerjoin`, but doesn’t add any columns from the 2nd table:

```jldoctest dataframe
julia> semijoin(persons, earnings, on = :id)
3×2 DataFrame
 Row │ id     name
     │ Int64  String
─────┼───────────────
   1 │     3  Rahul
   2 │     4  Vijay
   3 │     5  Akshat
```

The below example will return true, demonstrating that a `semijoin` is the same as `innerjoin` with 
only columns from persons data frame.

```jldoctest dataframe
julia> semijoin(persons, earnings, on = :id) == innerjoin(persons, earnings, on = :id)[:, names(persons)]
true
```

Now, let's have a look on `crossjoin`:

```jldoctest dataframe
julia> data = crossjoin(persons, earnings, makeunique = true)
30×4 DataFrame
 Row │ id     name    id_1   salary
     │ Int64  String  Int64  Int64
─────┼──────────────────────────────
   1 │     1  Rohit       3    5000
   2 │     1  Rohit       4    1000
   3 │     1  Rohit       5    5000
   4 │     1  Rohit       6    8000
   5 │     1  Rohit       7    3000
   6 │     1  Rohit       8    9000
   7 │     2  Mohit       3    5000
   8 │     2  Mohit       4    1000
  ⋮  │   ⋮      ⋮       ⋮      ⋮
  24 │     4  Vijay       8    9000
  25 │     5  Akshat      3    5000
  26 │     5  Akshat      4    1000
  27 │     5  Akshat      5    5000
  28 │     5  Akshat      6    8000
  29 │     5  Akshat      7    3000
  30 │     5  Akshat      8    9000
                     15 rows omitted
```

In the above example, A `crossjoin` took all the rows from `persons` data frame and for each row, it matches it 
to every single row of `earnings` data frame. At first impression, this might not make any sense, but it’s a great 
method for finding all combinations of 2 data frames. Our new data frame has 30 rows, which is 
5 (rows of persons) x 6 (rows of earnings).

!!! note

  Cross joins are the only kind of join that does not use a `on` key:
  `crossjoin(persons, earnings, makeunique = true)`

For more clarification, now let’s say we want to design a new data frame by changing the ingredients. To 
understand profitability, we also need to figure out the total costs of the chocolate:

```jldoctest dataframe
julia> base_layer = DataFrame(base = ["biscuit", "chocolate biscuit", "marshmallow"], 
                              base_cost = [0.05, 0.08, 0.03]) # have table of bases
3×2 DataFrame
 Row │ base               base_cost
     │ String             Float64
─────┼──────────────────────────────
   1 │ biscuit                 0.05
   2 │ chocolate biscuit       0.08
   3 │ marshmallow             0.03
    
julia> coating_layer = DataFrame(coating = ["caramel", "chocolate sauce"],
                                 coating_cost = [0.01, 0.05]) # table of coatings
2×2 DataFrame
 Row │ coating          coating_cost
     │ String           Float64
─────┼───────────────────────────────
   1 │ caramel                  0.01
   2 │ chocolate sauce          0.05

julia> innovation_table = crossjoin(base_layer, coating_layer) # do the join
6×4 DataFrame
 Row │ base               base_cost  coating          coating_cost
     │ String             Float64    String           Float64
─────┼─────────────────────────────────────────────────────────────
   1 │ biscuit                 0.05  caramel                  0.01
   2 │ biscuit                 0.05  chocolate sauce          0.05
   3 │ chocolate biscuit       0.08  caramel                  0.01
   4 │ chocolate biscuit       0.08  chocolate sauce          0.05
   5 │ marshmallow             0.03  caramel                  0.01
   6 │ marshmallow             0.03  chocolate sauce          0.05

julia> innovation_table.total_cost = innovation_table.base_cost .+ innovation_table.coating_cost # add together the 2 cost columns to calculate total cost
6-element Vector{Float64}:
 0.060000000000000005
 0.1
 0.09
 0.13
 0.04
 0.08

julia> innovation_table
6×5 DataFrame
 Row │ base               base_cost  coating          coating_cost  total_cost ⋯
     │ String             Float64    String           Float64       Float64    ⋯
─────┼──────────────────────────────────────────────────────────────────────────
   1 │ biscuit                 0.05  caramel                  0.01        0.06 ⋯
   2 │ biscuit                 0.05  chocolate sauce          0.05        0.1
   3 │ chocolate biscuit       0.08  caramel                  0.01        0.09
   4 │ chocolate biscuit       0.08  chocolate sauce          0.05        0.13
   5 │ marshmallow             0.03  caramel                  0.01        0.04 ⋯
   6 │ marshmallow             0.03  chocolate sauce          0.05        0.08
```

## Multiple keys to join on

Now that we have better chocolate, let’s learn how to `join` on multiple columns. Extending the 
above joins to work with *2 keys* is very easy. In fact, all we have to do is pass a vector of 
symbols or strings to the `on` argument of the `join` functions. To demonstrate this, let’s copy 
and add another column to both of our data frames. This will contain city names where the users live.

```jldoctest dataframe
julia> C_names= copy(persons) # It make sure we don't mess with the original table
5×2 DataFrame
 Row │ id     name
     │ Int64  String
─────┼───────────────
   1 │     1  Rohit
   2 │     2  Mohit
   3 │     3  Rahul
   4 │     4  Vijay
   5 │     5  Akshat

julia> C_names.city = ["Orai", "Gwalior"][mod1.(1:5, 2)]
5-element Vector{String}:
 "Orai"
 "Gwalior"
 "Orai"
 "Gwalior"
 "Orai"

julia> D_earnings = copy(earnings) # do the same for earnings
6×2 DataFrame
 Row │ id     salary
     │ Int64  Int64
─────┼───────────────
   1 │     3    5000
   2 │     4    1000
   3 │     5    5000
   4 │     6    8000
   5 │     7    3000
   6 │     8    9000

julia> D_earnings.city = ["Orai", "Gwalior"][mod1.(1:6, 2)]
6-element Vector{String}:
 "Orai"
 "Gwalior"
 "Orai"
 "Gwalior"
 "Orai"
 "Gwalior"

julia> C_names
5×3 DataFrame
 Row │ id     name    city
     │ Int64  String  String
─────┼────────────────────────
   1 │     1  Rohit   Orai
   2 │     2  Mohit   Gwalior
   3 │     3  Rahul   Orai
   4 │     4  Vijay   Gwalior
   5 │     5  Akshat  Orai

julia> D_earnings
6×3 DataFrame
 Row │ id     salary  city
     │ Int64  Int64   String
─────┼────────────────────────
   1 │     3    5000  Orai
   2 │     4    1000  Gwalior
   3 │     5    5000  Orai
   4 │     6    8000  Gwalior
   5 │     7    3000  Orai
   6 │     8    9000  Gwalior
```

One way we can think of this is that we have 2 separate data frames. One in Orai and another Gwalior. 
With the systems not knowing about each other, they keep track of the users’ `:id`s separately. So the 
name of user 1 in Orai is not the same as user 1 in Gwalior. Indeed they are different users! So 
when we merge the 2 data frames we want to make sure that the names and earnings are not only matched in 
user `:id`s but also on the database name. Let’s do some joining on both columns then:

```jldoctest dataframe
julia> innerjoin(C_names, D_earnings, on = [:id, :city])
3×4 DataFrame
 Row │ id     name    city     salary
     │ Int64  String  String   Int64
─────┼────────────────────────────────
   1 │     3  Rahul   Orai       5000
   2 │     4  Vijay   Gwalior    1000
   3 │     5  Akshat  Orai       5000
```

As we can see, only Rahul appears in both data frames and lives in the same place.

## Different column names

One problem that we might face is that our key columns don’t have the same names across our data frames. 
If that’s the case then we have 2 options:
- we can rename the columns using `rename!`
- or we can pass a mapping of names as the `on` parameter

```jldoctest dataframe
julia> another_earnings = DataFrame(another_id = 3:8, salary = [1500, 1600, 1700, 1800, 1900, 2800])
6×2 DataFrame
 Row │ another_id  salary
     │ Int64       Int64
─────┼────────────────────
   1 │          3    1500
   2 │          4    1600
   3 │          5    1700
   4 │          6    1800
   5 │          7    1900
   6 │          8    2800
```

Now what we want to do is to join the new earnings data frame with the names but we want to use `:another_id`
in the new data frame as our joining key.

```jldoctest dataframe
julia> innerjoin(persons, another_earnings, on = :id => :another_id)
3×3 DataFrame
 Row │ id     name    salary
     │ Int64  String  Int64
─────┼───────────────────────
   1 │     3  Rahul     1500
   2 │     4  Vijay     1600
   3 │     5  Akshat    1700
```

Here we told Julia to map `=>` the `:id` column to the `:another_id` column when doing the joins. This same 
exact format is used when we need to rename columns.

Here is another example with multiple columns:

```jldoctest dataframe
julia> a = DataFrame(City = ["Gwalior", "Delhi", "Delhi", "Mumbai", "Mumbai"],
                     Job = ["Lawyer", "Lawyer", "Lawyer", "Doctor", "Doctor"],
                     Category = [1, 2, 3, 4, 5])
5×3 DataFrame
 Row │ City     Job     Category
     │ String   String  Int64
─────┼───────────────────────────
   1 │ Gwalior  Lawyer         1
   2 │ Delhi    Lawyer         2
   3 │ Delhi    Lawyer         3
   4 │ Mumbai   Doctor         4
   5 │ Mumbai   Doctor         5

julia> b = DataFrame(Location = ["Banglore", "Delhi", "Delhi", "Mumbai", "Mumbai"],
                     Work = ["Lawyer", "Lawyer", "Lawyer", "Doctor", "Doctor"],
                     Name = ["a", "b", "c", "d", "e"])
5×3 DataFrame
 Row │ Location  Work    Name
     │ String    String  String
─────┼──────────────────────────
   1 │ Banglore  Lawyer  a
   2 │ Delhi     Lawyer  b
   3 │ Delhi     Lawyer  c
   4 │ Mumbai    Doctor  d
   5 │ Mumbai    Doctor  e

julia> innerjoin(a, b, on = [:City => :Location, :Job => :Work])
8×4 DataFrame
 Row │ City    Job     Category  Name
     │ String  String  Int64     String
─────┼──────────────────────────────────
   1 │ Delhi   Lawyer         2  b
   2 │ Delhi   Lawyer         3  b
   3 │ Delhi   Lawyer         2  c
   4 │ Delhi   Lawyer         3  c
   5 │ Mumbai  Doctor         4  d
   6 │ Mumbai  Doctor         5  d
   7 │ Mumbai  Doctor         4  e
   8 │ Mumbai  Doctor         5  e
```

Additionally, notice that in the last join rows 2 and 3 had the same values on `on` variables in both joined 
data frames. In such a situation `innerjoin`, `outerjoin`, `leftjoin` and `rightjoin` will produce all combinations 
of matching rows. In our example rows from 2 to 5 were created as a result. The same behavior can be observed 
for rows 4 and 5 in both joined DataFrames.

In order to check that columns passed as the `on` argument define unique keys (according to `isequal`) in each 
input data frame you can set the `validate` keyword argument to a two-element tuple or a pair of `Bool` values, 
with each element indicating whether to run check for the corresponding data frame. Here is an example for the 
join operation described above:

```jldoctest dataframe
julia> innerjoin(a, b, on = [(:City => :Location), (:Job => :Work)], validate=(true, true))
ERROR: ArgumentError: Merge key(s) are not unique in both df1 and df2. df1 contains 2 duplicate keys: (City = "Delhi", Job = "Lawyer") and (City = "Mumbai", Job = "Doctor"). df2 contains 2 duplicate keys: (Location = "Delhi", Work = "Lawyer") and (Location = "Mumbai", Work = "Doctor").
```

Finally, using the `source` keyword argument we can add a column to the resulting data frame indicating whether 
the given row appeared only in the left, the right or both data frames. Here is an example:

```jldoctest dataframe
julia> persons = DataFrame(id = 1:5, name = ["Rohit", "Mohit", "Rahul", "Vijay", "Akshat"])
5×2 DataFrame
 Row │ id     name
     │ Int64  String
─────┼───────────────
   1 │     1  Rohit
   2 │     2  Mohit
   3 │     3  Rahul
   4 │     4  Vijay
   5 │     5  Akshat

julia> earnings = DataFrame(id = 3:8, salary = [5000, 1000, 5000, 8000, 3000, 9000])
6×2 DataFrame
 Row │ id     salary
     │ Int64  Int64
─────┼───────────────
   1 │     3    5000
   2 │     4    1000
   3 │     5    5000
   4 │     6    8000
   5 │     7    3000
   6 │     8    9000

julia> outerjoin(persons, earnings, on=:id, validate=(true, true), source=:source)
8×4 DataFrame
 Row │ id     name     salary   source
     │ Int64  String?  Int64?   String
─────┼─────────────────────────────────────
   1 │     3  Rahul       5000  both
   2 │     4  Vijay       1000  both
   3 │     5  Akshat      5000  both
   4 │     1  Rohit    missing  left_only
   5 │     2  Mohit    missing  left_only
   6 │     6  missing     8000  right_only
   7 │     7  missing     3000  right_only
   8 │     8  missing     9000  right_only
```

Note that this time we also used the `validate` keyword argument and it did not produce errors as the keys 
defined in both source data frames were unique.