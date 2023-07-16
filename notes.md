# Why does `:` only sometimes work?
I don't like `:` being listed as a `source_column_selector` when it often errors.

`select(df, :)`
: works

`transform(df, Cols(:) => ByRow(max))`
: works

`transform(df, : => ByRow(max))` errors
: ERROR: syntax: whitespace not allowed after ":" used for quoting

Similarly, regular expressions also sometimes must be wrapped in `Cols`.
This seems inconsistent.
I don't understand the rules for when `Cols` is and isn't needed.
It should be explained in the documentation.

```julia
julia> df = DataFrame(Time=[3, 4, 5], TopTemp=[70, 73, 100], BottomTemp=[50, 55, 80])
3×3 DataFrame
 Row │ Time   TopTemp  BottomTemp
     │ Int64  Int64    Int64
─────┼────────────────────────────
   1 │     3       70          50
   2 │     4       73          55
   3 │     5      100          80

julia> transform(df, Cols(r"Temp") .=> (t->(t.-32)*5/9), renamecols=false)
3×3 DataFrame
 Row │ Time   TopTemp  BottomTemp
     │ Int64  Float64  Float64
─────┼────────────────────────────
   1 │     3  21.1111     10.0
   2 │     4  22.7778     12.7778
   3 │     5  37.7778     26.6667

julia> transform(df, r"Temp" .=> (t->(t.-32)*5/9), renamecols=false) # breaks without Cols
ERROR: MethodError: no method matching (::var"#5#6")(::Vector{Int64}, ::Vector{Int64})
```
```julia
julia> df = DataFrame([1 2 3 4], :auto)
1×4 DataFrame
 Row │ x1     x2     x3     x4
     │ Int64  Int64  Int64  Int64
─────┼────────────────────────────
   1 │     1      2      3      4

julia> select(df, r"x" => ByRow(min)) # works without Cols
1×1 DataFrame
 Row │ x1_x2_etc_min
     │ Int64
─────┼───────────────
   1 │             1
```

# Cannot pack columns into a named tuple column?
If I can do this to unpack named tuples:
```julia
julia> df = DataFrame(in=[(a=1,b=2), (a=3, b=4)])
2×1 DataFrame
 Row │ in
     │ NamedTup…
─────┼────────────────
   1 │ (a = 1, b = 2)
   2 │ (a = 3, b = 4)

julia> df2 = select(df, :in => AsTable)
2×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      2
   2 │     3      4
```
Then I should also be able to do the reverse to pack named tuples and get `df` back:
```julia
julia> df3 = select(df2, AsTable(:) => :in)
ERROR: ArgumentError: Unrecognized column selector: AsTable(Colon()) => :in
```

# Why are types truncated when there is still a lot of room?

Also, why is `NamedTup...` truncated in the output above? There is room.

# AsTable cannot be used on it's own?
Maybe it should return a NamedTuple?

```julia
julia> select(df, :first_name)
3×1 DataFrame
 Row │ first_name
     │ String
─────┼────────────
   1 │ José
   2 │ Emma
   3 │ Nathan

julia> select(df, AsTable(:first_name))
ERROR: ArgumentError: Unrecognized column selector: AsTable(:first_name)

julia> select(df, :)
3×4 DataFrame
 Row │ id     first_name  last_name  age
     │ Int64  String      String     Int64
─────┼─────────────────────────────────────
   1 │     1  José        Garcia        61
   2 │     2  Emma        Marino        24
   3 │     3  Nathan      Boyer         33

julia> select(df, AsTable(:))
ERROR: ArgumentError: Unrecognized column selector: AsTable(Colon())
```

# Is summary table correct?
Can `subset` ever create new columns?
Can `combine` ever create more than one row?

# Add renaming function method to `rename`?
I understand why this cannot work for the manipulation functions,
but it seems like this method would be useful for `rename` and `rename!`.
```julia
julia> df = DataFrame(a=1:4, b=5:8)
4×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      5
   2 │     2      6
   3 │     3      7
   4 │     4      8

julia> add_new(s) = "new_" * s
add_new (generic function with 1 method)

julia> rename(df, :a => add_new)
ERROR: MethodError: no method matching rename!(::DataFrame, ::Vector{Pair{Symbol, typeof(add_new)}})
```

I would like it to behave as below:
```julia
julia> rename(df, :a => add_new)
4×2 DataFrame
 Row │ new_a  b
     │ Int64  Int64
─────┼──────────────
   1 │     1      5
   2 │     2      6
   3 │     3      7
   4 │     4      8
```

This works, but can only be applied to the entire data frame.
```julia
julia> rename(add_new, df)
4×2 DataFrame
 Row │ new_a  new_b
     │ Int64  Int64
─────┼──────────────
   1 │     1      5
   2 │     2      6
   3 │     3      7
   4 │     4      8
```

# Cannot automatically group from NamedTuples?
I am suprised that this does not work:
```julia
julia> df = DataFrame(data = [(a=1,b=2), (b=3,a=4)]) # vector of `NamedTuple`s
2×1 DataFrame
 Row │ data
     │ NamedTup…
─────┼────────────────
   1 │ (a = 1, b = 2)
   2 │ (b = 3, a = 4)

julia> transform(df, :data => AsTable) # breaks if tuples not all in the same order
ERROR: ArgumentError: keys of the returned elements must be identical
```

when these do work:
```julia
julia> df2 = DataFrame(data = [(a=1,b=2), (a=3,b=4)]) # vector of `NamedTuple`s
2×1 DataFrame
 Row │ data
     │ NamedTup…
─────┼────────────────
   1 │ (a = 1, b = 2)
   2 │ (a = 3, b = 4)

julia> transform(df2, :data => AsTable) # works if tuples are in the same order
2×3 DataFrame
 Row │ data            a      b
     │ NamedTup…       Int64  Int64
─────┼──────────────────────────────
   1 │ (a = 1, b = 2)      1      2
   2 │ (a = 3, b = 4)      3      4
```

```julia
julia> df = DataFrame(data = [(a=1,b=2), (b=3,a=4)]) # vector of `NamedTuple`s
2×1 DataFrame
 Row │ data
     │ NamedTup…
─────┼────────────────
   1 │ (a = 1, b = 2)
   2 │ (b = 3, a = 4)

julia> getfield.(df.data, :a) # getfield can sort out named tuples
2-element Vector{Int64}:
 1
 4

julia> (; a, b) = df[2, :data] # tuple destructuring can sort out named tuples
(b = 3, a = 4)
```
