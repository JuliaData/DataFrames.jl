# Joining DataFrames

"You may have two different data sets in Julia but you may not be sure how to perform *joins*. What 
criteria should I consider? What are the different ways I can join these data sets?"

!!! note

    Some of the part has been taken from [Bence Komarniczky](https://towardsdatascience.com/joining-dataframes-in-julia-c435e3da32f3)

Sound familiar? You may have come across this question plenty of times on online discussion
forums. Working with one data frame is fairly straightforward but things become challenging when
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

julia> german = view(german_ref, 1:10, 1:5)
10×5 SubDataFrame
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
   9 │     8     61  male        1  own
  10 │     9     28  male        3  own
```

This table has all the `:id`s of all factors to make comparisions for risk factor. Let’s say we 
have another data frame, which has the risk (good or bad) of these individuals:

```jldoctest dataframe
julia> risk_ref = CSV.read(joinpath(dirname(pathof(DataFrames)),
                                    "..", "docs", "src", "assets", "risk.csv"),
                           DataFrame);

julia> risk = view(risk_ref, 1:10, :)
10×2 SubDataFrame
 Row │ id     Risk
     │ Int64  String
─────┼───────────────
   1 │     0  good
   2 │     1  bad
   3 │     2  good
   4 │     3  good
   5 │     4  bad
   6 │     5  good
   7 │     6  good
   8 │     7  good
   9 │     8  good
  10 │     9  bad
```

We now have 2 data frames:
- `german_ref` holds the `:id`s, `:Age`, `:Sex`, `:Job` and `:Housing`
- `risk_ref` holds the `:id`s and `:Risk`.

In relational database theory, to combine two datasets referred to as a join. The columns used to 
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


## Inner Join

The output contains rows for values of the key that exist in both the first (left) and second (right) 
arguments to join.

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
julia> innerjoin(german, risk, on = :id)
10×6 DataFrame
 Row │ id     Age    Sex     Job    Housing  Risk
     │ Int64  Int64  String  Int64  String   String
─────┼──────────────────────────────────────────────
   1 │     0     67  male        2  own      good
   2 │     1     22  female      2  own      bad
   3 │     2     49  male        1  own      good
   4 │     3     45  male        2  free     good
   5 │     4     53  male        2  free     bad
   6 │     5     35  male        1  free     good
   7 │     6     53  male        2  own      good
   8 │     7     35  male        3  rent     good
   9 │     8     61  male        1  own      good
  10 │     9     28  male        3  own      bad                                          
```

Let’s go through this in detail. Arguments 1 and 2 `german_ref` & `risk_ref` are the two tables we’re 
joining. Argument 3 `on` tells us what the key column is. We’ll use this column to match the observations 
across the data frames. As you can see, we ended up with *1000 rows* and *11 columns*. Now go back to the 
beginning and check out how the 2 original data frames looked like. Make sure you understand why we ended up 
with these rows only. Since we’re using `innerjoin`, we’re only keeping `:id`s that appear in both data frames — 
everything else we throw away.

## Left Join

The output contains rows for values of the key that exist in the first (left) argument to join, whether 
or not that value exists in the second (right) argument.

Now, let’s do a `leftjoin` and see what happens to `:id`s that are only in the first data frame:

```jldoctest dataframe
julia> risk1 = view(risk_ref, 5:15, :)
11×2 SubDataFrame
 Row │ id     Risk
     │ Int64  String
─────┼───────────────
   1 │     4  bad
   2 │     5  good
   3 │     6  good
   4 │     7  good
   5 │     8  good
   6 │     9  bad
   7 │    10  bad
   8 │    11  bad
   9 │    12  good
  10 │    13  bad
  11 │    14  good
```

This data frame has all the `:id`s of individuals and their risk factors. 

Now, lets perform `leftjoin` operation:

```jldoctest dataframe
julia> leftjoin(german, risk1, on = :id)
10×6 DataFrame
 Row │ id     Age    Sex     Job    Housing  Risk
     │ Int64  Int64  String  Int64  String   String?
─────┼───────────────────────────────────────────────
   1 │     4     53  male        2  free     bad
   2 │     5     35  male        1  free     good
   3 │     6     53  male        2  own      good
   4 │     7     35  male        3  rent     good
   5 │     8     61  male        1  own      good
   6 │     9     28  male        3  own      bad
   7 │     0     67  male        2  own      missing
   8 │     1     22  female      2  own      missing
   9 │     2     49  male        1  own      missing
  10 │     3     45  male        2  free     missing
```

Here we kept all of the observations from data frame `german` no matter what is going on in data frame `risk1`. 
For records without a match in data frame `german`, the `:Risk` column has `missingas` value. This makes sense 
as we never actually saw those risk figures. Of course, there is also a `rightjoin`. This keeps all rows 
from the second data frame.

## Right Join

The output contains rows for values of the key that exist in the second (right) argument to join, 
whether or not that value exists in the first (left) argument.

As above we discussed about *Left Join* which were keeping all of the observations from data frame
`german` no matter what is going on in data frame `risk1` but *Right Join* keeps all rows from the
`risk1` data frame. Lets see an example:

```jldoctest dataframe
julia> rightjoin(german, risk1, on = :id)
11×6 DataFrame
 Row │ id     Age      Sex      Job      Housing  Risk
     │ Int64  Int64?   String?  Int64?   String?  String
─────┼───────────────────────────────────────────────────
   1 │     4       53  male           2  free     bad
   2 │     5       35  male           1  free     good
   3 │     6       53  male           2  own      good
   4 │     7       35  male           3  rent     good
   5 │     8       61  male           1  own      good
   6 │     9       28  male           3  own      bad
   7 │    10  missing  missing  missing  missing  bad
   8 │    11  missing  missing  missing  missing  bad
   9 │    12  missing  missing  missing  missing  good
  10 │    13  missing  missing  missing  missing  bad
  11 │    14  missing  missing  missing  missing  good
```

## Outer Join

The output contains rows for values of the key that exist in the first (left) or second (right) 
argument to join.

If we want to have all `:id`s from both data frames, we use an `outerjoin`:

```jldoctest dataframe
julia> outerjoin(german, risk1, on = :id)
15×6 DataFrame
 Row │ id     Age      Sex      Job      Housing  Risk
     │ Int64  Int64?   String?  Int64?   String?  String?
─────┼────────────────────────────────────────────────────
   1 │     4       53  male           2  free     bad
   2 │     5       35  male           1  free     good
   3 │     6       53  male           2  own      good
   4 │     7       35  male           3  rent     good
   5 │     8       61  male           1  own      good
   6 │     9       28  male           3  own      bad
   7 │     0       67  male           2  own      missing
   8 │     1       22  female         2  own      missing
   9 │     2       49  male           1  own      missing
  10 │     3       45  male           2  free     missing
  11 │    10  missing  missing  missing  missing  bad
  12 │    11  missing  missing  missing  missing  bad
  13 │    12  missing  missing  missing  missing  good
  14 │    13  missing  missing  missing  missing  bad
  15 │    14  missing  missing  missing  missing  good
```

It loads more `missing` values, but we can see all the `:id`s now. It check out the new `missing` 
names for `:id`s 10-12. It is known as `outerjoin`.

The above four joins make up the basics of data frame merging. We can remember like this:
- `innerjoin`: The output contains rows for values of the key that exist in both the first (left) and 
  second (right) arguments to `join`.
- `leftjoin`: The output contains rows for values of the key that exist in the first (left) argument to 
  `join`, whether or not that value exists in the second (right) argument.
- `rightjoin`: The output contains rows for values of the key that exist in the second (right) argument 
  to `join`, whether or not that value exists in the first (left) argument.
- `outerjoin`: The output contains rows for values of the key that exist in the first (left) or second 
  (right) argument to `join`.

## Anti Join

The output contains rows for values of the key that exist in the first (left) but not the second 
(right) argument to join. As with semi joins, output is restricted to columns from the first (left) 
argument.

Finally, we can keep only rows that *don't* match with `antijoin`:

```jldoctest dataframe
julia> antijoin(german, risk1, on = :id)
4×5 DataFrame
 Row │ id     Age    Sex     Job    Housing
     │ Int64  Int64  String  Int64  String
─────┼──────────────────────────────────────
   1 │     0     67  male        2  own
   2 │     1     22  female      2  own
   3 │     2     49  male        1  own
   4 │     3     45  male        2  free
```

## Semi Join

It is like an inner join, but output is restricted to columns from the first (left) argument to join.

Now, let’s say we want to look at german’s data for whom we have risk data, but we don’t actually 
want to have all the columns from the second table. That’s what `semijoin` does. It gives us the same 
rows as an `innerjoin`, but doesn’t add any columns from the 2nd table:

```jldoctest dataframe
julia> semijoin(german, risk1, on = :id)
6×5 DataFrame
 Row │ id     Age    Sex     Job    Housing
     │ Int64  Int64  String  Int64  String
─────┼──────────────────────────────────────
   1 │     4     53  male        2  free
   2 │     5     35  male        1  free
   3 │     6     53  male        2  own
   4 │     7     35  male        3  rent
   5 │     8     61  male        1  own
   6 │     9     28  male        3  own
```

The below example will return true, demonstrating that a `semijoin` is the same as `innerjoin` with 
only columns from `german` data frame.

```jldoctest dataframe
julia> semijoin(german, risk, on = :id) == innerjoin(german, risk, on = :id)[:, names(german)]
true
```

## Cross Join

The output is the cartesian product of rows from the first (left) and second (right) arguments to join.

Now, let's have a look on `crossjoin`:

```jldoctest dataframe
julia> crossjoin(german, risk, makeunique=true);
```

In the above example, A `crossjoin` took all the rows from `german` data frame and for each row, it matches it 
to every single row of `risk` data frame. At first impression, this might not make any sense, but it’s a great 
method for finding all combinations of 2 data frames. Our new data frame has 100 rows.

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
julia> C_names= copy(german) # It make sure we don't mess with the original table
10×5 DataFrame
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
   9 │     8     61  male        1  own
  10 │     9     28  male        3  own

julia> C_names.city = ["Orai", "Gwalior"][mod1.(1:10, 2)]
10-element Vector{String}:
 "Orai"
 "Gwalior"
 "Orai"
 "Gwalior"
 "Orai"
 "Gwalior"
 "Orai"
 "Gwalior"
 "Orai"
 "Gwalior"

julia> D_earnings = copy(risk) # do the same for earnings
10×2 DataFrame
 Row │ id     Risk
     │ Int64  String
─────┼───────────────
   1 │     0  good
   2 │     1  bad
   3 │     2  good
   4 │     3  good
   5 │     4  bad
   6 │     5  good
   7 │     6  good
   8 │     7  good
   9 │     8  good
  10 │     9  bad

julia> D_earnings.city = ["Orai", "Gwalior"][mod1.(1:10, 2)]
10-element Vector{String}:
 "Orai"
 "Gwalior"
 "Orai"
 "Gwalior"
 "Orai"
 "Gwalior"
 "Orai"
 "Gwalior"
 "Orai"
 "Gwalior"

julia> C_names
10×6 DataFrame
 Row │ id     Age    Sex     Job    Housing  city
     │ Int64  Int64  String  Int64  String   String
─────┼───────────────────────────────────────────────
   1 │     0     67  male        2  own      Orai
   2 │     1     22  female      2  own      Gwalior
   3 │     2     49  male        1  own      Orai
   4 │     3     45  male        2  free     Gwalior
   5 │     4     53  male        2  free     Orai
   6 │     5     35  male        1  free     Gwalior
   7 │     6     53  male        2  own      Orai
   8 │     7     35  male        3  rent     Gwalior
   9 │     8     61  male        1  own      Orai
  10 │     9     28  male        3  own      Gwalior

julia> D_earnings
10×3 DataFrame
 Row │ id     Risk    city
     │ Int64  String  String
─────┼────────────────────────
   1 │     0  good    Orai
   2 │     1  bad     Gwalior
   3 │     2  good    Orai
   4 │     3  good    Gwalior
   5 │     4  bad     Orai
   6 │     5  good    Gwalior
   7 │     6  good    Orai
   8 │     7  good    Gwalior
   9 │     8  good    Orai
  10 │     9  bad     Gwalior
```

One way we can think of this is that we have 2 separate data frames. One in Orai and another Gwalior. 
With the systems not knowing about each other, they keep track of the users’ `:id`s separately. So the 
name of user 1 in Orai is not the same as user 1 in Gwalior. Indeed they are different users! So 
when we merge the 2 data frames we want to make sure that the names and earnings are not only matched in 
user `:id`s but also on the database name. Let’s do some joining on both columns then:

```jldoctest dataframe
julia> innerjoin(C_names, D_earnings, on = [:id, :city])
10×7 DataFrame
 Row │ id     Age    Sex     Job    Housing  city     Risk
     │ Int64  Int64  String  Int64  String   String   String
─────┼───────────────────────────────────────────────────────
   1 │     0     67  male        2  own      Orai     good
   2 │     1     22  female      2  own      Gwalior  bad
   3 │     2     49  male        1  own      Orai     good
   4 │     3     45  male        2  free     Gwalior  good
   5 │     4     53  male        2  free     Orai     bad
   6 │     5     35  male        1  free     Gwalior  good
   7 │     6     53  male        2  own      Orai     good
   8 │     7     35  male        3  rent     Gwalior  good
   9 │     8     61  male        1  own      Orai     good
  10 │     9     28  male        3  own      Gwalior  bad
```

## Different column names

One problem that we might face is that our key columns don’t have the same names across our data frames. 
If that’s the case then we have 2 options:
- we can rename the columns using `rename!`
- or we can pass a mapping of names as the `on` parameter

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

## `matchmissing` keyword argument

Now we have enough idea about kind of joins. In this section you may see any join at any time. So,
we are considering that you will be comfortable with the different kind of Joins which we have 
discussed above. 

The `matchmissing` keyword argument allows us to decide how `missing` value is handled in `on`
columns in joins. In general you have three options:

- `:error` (the default): throw an error if `missing` value is present in any of the `on` columns;
  the rationale is that `missing` indicates unknown value so if we knew it could match to any of 
  the non-missing values in the `on` columns in the other data frame we join;
- `:equal`: `missing` values are allowed and they are matched to `missing` values only; in this scenario
  we treat `missing` as any other value without giving it a special treatment;
- `:notequal` (a new option): in this case `missing` is considered to be not equal to any other value
  (including `missing`).

But there are some consequences of the `:notequal` rule. In `innerjoin` this means that rows with `missing`
values will be dropped both in left and right table. In `leftjoin`, `semijoin`, and `antijoin` they are 
dropped from the right table only (which means that if `missing` is present in the left table it is retained
in processing but considered not to match any row in right table). Similarly in `rightjoin` rows with `missing`
are dropped from left table only. The case that is most difficult to handle is `outerjoin`. The reason is that
if `missing` would be present in both left and right table they would be considered not equal and produced 
separate rows in the output table. 

Lets move to the examples showing `matchmissing`:

Here, we are creating a *SubDataFrame* from our parent dataframe `german_ref` to include `missing`
values as well.

```jldoctest dataframe
julia> df1 = view(german_ref, 1:16, ["id", "Saving accounts", "Checking account"])
16×3 SubDataFrame
 Row │ id     Saving accounts  Checking account
     │ Int64  String?          String?
─────┼──────────────────────────────────────────
   1 │     0  missing          little
   2 │     1  little           moderate
   3 │     2  little           missing
   4 │     3  little           little
   5 │     4  little           little
   6 │     5  missing          missing
   7 │     6  quite rich       missing
   8 │     7  little           moderate
   9 │     8  rich             missing
  10 │     9  little           moderate
  11 │    10  little           moderate
  12 │    11  little           little
  13 │    12  little           moderate
  14 │    13  little           little
  15 │    14  little           little
  16 │    15  moderate         little

julia> df2 = view(risk_ref, 10:26, :)
17×2 SubDataFrame
 Row │ id     Risk
     │ Int64  String
─────┼───────────────
   1 │     9  bad
   2 │    10  bad
   3 │    11  bad
   4 │    12  good
   5 │    13  bad
   6 │    14  good
   7 │    15  bad
   8 │    16  good
   9 │    17  good
  10 │    18  bad
  11 │    19  good
  12 │    20  good
  13 │    21  good
  14 │    22  good
  15 │    23  good
  16 │    24  good
  17 │    25  good
```

Now, we will investigate all the possible join operations:

```jldoctest dataframe
julia> innerjoin(df1, df2, on=:id, matchmissing=:notequal)
7×4 DataFrame
 Row │ id     Saving accounts  Checking account  Risk
     │ Int64  String?          String?           String
─────┼──────────────────────────────────────────────────
   1 │     9  little           moderate          bad
   2 │    10  little           moderate          bad
   3 │    11  little           little            bad
   4 │    12  little           moderate          good
   5 │    13  little           little            bad
   6 │    14  little           little            good
   7 │    15  moderate         little            bad
```

As you can see for `innerjoin` only rows with `:id` equal to `9` to `15` are retained. Let us
move forward:

```jldoctest dataframe
julia> leftjoin(df1, df2, on=:id, matchmissing=:notequal, source=:source)
16×5 DataFrame
 Row │ id     Saving accounts  Checking account  Risk     source
     │ Int64  String?          String?           String?  String
─────┼──────────────────────────────────────────────────────────────
   1 │     9  little           moderate          bad      both
   2 │    10  little           moderate          bad      both
   3 │    11  little           little            bad      both
   4 │    12  little           moderate          good     both
   5 │    13  little           little            bad      both
   6 │    14  little           little            good     both
   7 │    15  moderate         little            bad      both
   8 │     0  missing          little            missing  left_only
   9 │     1  little           moderate          missing  left_only
  10 │     2  little           missing           missing  left_only
  11 │     3  little           little            missing  left_only
  12 │     4  little           little            missing  left_only
  13 │     5  missing          missing           missing  left_only
  14 │     6  quite rich       missing           missing  left_only
  15 │     7  little           moderate          missing  left_only
  16 │     8  rich             missing           missing  left_only

julia> rightjoin(df1, df2, on=:id, matchmissing=:notequal, source=:source)
17×5 DataFrame
 Row │ id     Saving accounts  Checking account  Risk    source
     │ Int64  String?          String?           String  String
─────┼──────────────────────────────────────────────────────────────
   1 │     9  little           moderate          bad     both
   2 │    10  little           moderate          bad     both
   3 │    11  little           little            bad     both
   4 │    12  little           moderate          good    both
   5 │    13  little           little            bad     both
   6 │    14  little           little            good    both
   7 │    15  moderate         little            bad     both
   8 │    16  missing          missing           good    right_only
   9 │    17  missing          missing           good    right_only
  10 │    18  missing          missing           bad     right_only
  11 │    19  missing          missing           good    right_only
  12 │    20  missing          missing           good    right_only
  13 │    21  missing          missing           good    right_only
  14 │    22  missing          missing           good    right_only
  15 │    23  missing          missing           good    right_only
  16 │    24  missing          missing           good    right_only
  17 │    25  missing          missing           good    right_only
```

For `leftjoin` and `rightjoin` we retained `missing` but only in the table for which all 
rows must be retained. Therefore in `leftjoin` at `:id` 0  for `:Saving accounts` equal to 
`missing` but `:Risk` also equal to `missing` (signalling that there was no match which we 
can also see in `:source` column). The same happens for `rightjoin` at `:id` 16 `:Risk` is
`good` but `:Saving accounts` and `:Checking account` are `missing`. 

The same rules work with `semijoin` and `antijoin` as you can see here:

```jldoctest dataframe
julia> semijoin(df1, df2, on=:id, matchmissing=:notequal)
7×3 DataFrame
 Row │ id     Saving accounts  Checking account
     │ Int64  String?          String?
─────┼──────────────────────────────────────────
   1 │     9  little           moderate
   2 │    10  little           moderate
   3 │    11  little           little
   4 │    12  little           moderate
   5 │    13  little           little
   6 │    14  little           little
   7 │    15  moderate         little

julia> antijoin(df1, df2, on=:id, matchmissing=:notequal)
9×3 DataFrame
 Row │ id     Saving accounts  Checking account
     │ Int64  String?          String?
─────┼──────────────────────────────────────────
   1 │     0  missing          little
   2 │     1  little           moderate
   3 │     2  little           missing
   4 │     3  little           little
   5 │     4  little           little
   6 │     5  missing          missing
   7 │     6  quite rich       missing
   8 │     7  little           moderate
   9 │     8  rich             missing
```

But `outerjoin` just throws an error:

```jldoctest dataframe
julia> outerjoin(df1, df2, on=:id, matchmissing=:notequal)
ERROR: ArgumentError: matchmissing == :notequal for `outerjoin` is not allowed
```