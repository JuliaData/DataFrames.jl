# Replacing Data

Several approaches can be used to replace some values with others in a data
frame. Some apply the replacement to all values in a data frame, and others to
individual columns or subset of columns.

Do note that in-place replacement requires that the replacement value can be
converted to the column's element type. In particular, this implies that
replacing a value with `missing` requires a call to `allowmissing!` if the
column did not allow for missing values.

Replacement operations affecting a single column can be performed using `replace!`:
```jldoctest replace
julia> df = DataFrame(a = ["a", "None", "b", "None"], b = 1:4, c = ["None", "j", "k", "h"], d = ["x", "y", "None", "z"])
4×4 DataFrame
 Row │ a       b      c       d
     │ String  Int64  String  String
─────┼───────────────────────────────
   1 │ a           1  None    x
   2 │ None        2  j       y
   3 │ b           3  k       None
   4 │ None        4  h       z

julia> replace!(df.a, "None" => "c")
4-element Array{String,1}:
 "a"
 "c"
 "b"
 "c"

julia> df
4×4 DataFrame
 Row │ a       b      c       d
     │ String  Int64  String  String
─────┼───────────────────────────────
   1 │ a           1  None    x
   2 │ c           2  j       y
   3 │ b           3  k       None
   4 │ c           4  h       z
```

This is equivalent to `df.a = replace(df.a, "None" => "c")`, but operates
in-place, without allocating a new column vector.

Replacement operations on multiple columns or on the whole data frame can be
performed in-place using the broadcasting syntax:

```jldoctest replace
# replacement on a subset of columns [:c, :d]
julia> df[:, [:c, :d]] .= ifelse.(df[!, [:c, :d]] .== "None", "c", df[!, [:c, :d]])
4×2 SubDataFrame
 Row │ c       d
     │ String  String
─────┼────────────────
   1 │ c       x
   2 │ j       y
   3 │ k       c
   4 │ h       z

julia> df
4×4 DataFrame
 Row │ a       b      c       d
     │ String  Int64  String  String
─────┼───────────────────────────────
   1 │ a           1  c       x
   2 │ c           2  j       y
   3 │ b           3  k       c
   4 │ c           4  h       z

julia> df .= ifelse.(df .== "c", "None", df) # replacement on entire data frame

4×4 DataFrame
 Row │ a       b      c       d
     │ String  Int64  String  String
─────┼───────────────────────────────
   1 │ a           1  None    x
   2 │ None        2  j       y
   3 │ b           3  k       None
   4 │ None        4  h       z
```

Do note that in the above examples, changing `.=` to just `=` will allocate new
column vectors instead of applying the operation in-place.

When replacing values with `missing`, if the columns do not already allow for
missing values, one has to either avoid in-place operation and use `=` instead
of `.=`, or call `allowmissing!` beforehand:

```jldoctest replace
julia> df2 = ifelse.(df .== "None", missing, df) # do not operate in-place (`df = ` would also work)
4×4 DataFrame
 Row │ a        b      c        d
     │ String?  Int64  String?  String?
─────┼──────────────────────────────────
   1 │ a            1  missing  x
   2 │ missing      2  j        y
   3 │ b            3  k        missing
   4 │ missing      4  h        z

julia> allowmissing!(df) # operate in-place after allowing for missing
4×4 DataFrame
 Row │ a        b       c        d
     │ String?  Int64?  String?  String?
─────┼───────────────────────────────────
   1 │ a             1  None     x
   2 │ None          2  j        y
   3 │ b             3  k        None
   4 │ None          4  h        z

julia> df .= ifelse.(df .== "None", missing, df)
4×4 DataFrame
 Row │ a        b       c        d
     │ String?  Int64?  String?  String?
─────┼───────────────────────────────────
   1 │ a             1  missing  x
   2 │ missing       2  j        y
   3 │ b             3  k        missing
   4 │ missing       4  h        z
```