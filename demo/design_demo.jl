
# there are some warnings here that need to be addressed...
load("DataFrames.jl")
using DataFrames

# The current DataFrame type hierarchy relatively transparently implements
# the notion of a set of named, equal-length, heterogeneous-typed vectors that may include
# missing data.

# An AbstractDataFrame is something that can be indexed in a [rows,cols] syntax.
# It currently inherits Associative functionality, which may be too broad --
# additional experience will be necessary.

#abstract AbstractDataFrame <: Associative{Any,Any}

# The standard implementation of an ADF is a DataFrame. It has a set of columns
# and a set of column names that refer to the columns. (Note that the column
# names are stored in an Index data structure that allows fast lookups and 
# some clever features described below.) Column names are of type ByteString --
# either ASCIIString or UTF8String. The current implementation allows columns
# to be of any type, which may be too broad, as described below.

#type DataFrame <: AbstractDataFrame
#    columns::Vector{Any} 
#    colindex::Index
# ...

# a SubDataFrame is a lightweight wrapper around a DataFrame used most frequently in
# split/apply sorts of operations.
#type SubDataFrame <: AbstractDataFrame
#    parent::DataFrame
#    rows::Vector{Int} # maps from subdf row indexes to parent row indexes
# ...

# The typical column type is a DataVec, which inherits from AbstractDataVec. A 
# DataVec is a vector of some type, along with a Boolean vector that acts as
# a mask for missing data. The decision to use a mask instead of a particular
# value (e.g., NaN + a particular payload for floats) remains contentious, 
# as it is in all other statistical programming languages and frameworks. 
#type DataVec{T} <: AbstractDataVec{T}
#    data::Vector{T}
#    na::AbstractVector{Bool} 

# DataVecs can be built and used independently of DataFrames. For interactive
# use, the type-referencing constructor is handy, and the NA literal is supported.
dvint = DataVec[1, 2, NA, 4]
# DataVecs can also be contructed from existing arrays. Note that the existing
# array is referenced, not copied. In general, the DataFrames.jl philosophy is 
# similar to the overall Julia philosophy -- don't copy data unless requested by 
# the user or otherwise necessary.
x = [5:8]
dvint2 = DataVec(x)
dvint2[1] = 10
print(x)
# Note that assignment of NA to a DataVec does not affect the underlying
# vector, just the mask.
dvint2[2] = NA
print(dvint2)
print(x)

# an aspect of DataVecs that needs to be revisited and likely redesigned is that
# they contain meta-data specifying how the NAs should be treated under 
# certain operations. To create a new DataVec object (with the same data
# and mask objects) that looks as if the NAs are ignored, use naFilter; to 
# define a replacement value, use naReplace.
try sum(dvint) end # fails now -- eventually, this should work.
sum(naFilter(dvint))
sum(naReplace(dvint, 100))

# A PooledDataVec is an implementation of AbstractDataVec that uses indexes into
# a pool of values instead of an array of values, which is space efficient in 
# the case of a large number of repeated elements. A common use is repeated 
# strings. PDVs don't have a mask -- they can use a 0-index to represent the NA.
pdvstr = PooledDataVec["one", "one", "two", "two", NA, "one", "one"]
idump(pdvstr)  # show the internal structure of a PooledDataVec
# some operations, such as csvDataFrame which loads a CSV file into a DataFrame
# will try to created a PooledDataVec when possible.

# A DataFrame is a mix of DataVecs, PooledDataVecs, and (maybe) other vectors

# this useful constructor from an expression unpacks the assignments, converts the LHSs
# to column names, and converts the RHS to DataVecs.
df1 = DataFrame(quote
    a = shuffle([1:10])
    b = ["A","B"][randi(2,10)]
    v1 = randn(10)
end)
typeof(df1["a"])

# Let's look at the structure of the data (`dump` is like R's `str`).
dump(df1)

# The names of the columns in a DF are stored in a new type called an Index (possibly
# needs a less-generic name?)
# an AbstractIndex is a thing that can be used to look up ordered things by name, but that
# will also accept a position or set of positions or range or other things and pass them
# through cleanly.
# This makes referencing columns by name or position very simple and flexible.
ii = Index(["cat", "dog", "bird", "turtle"])
ii["cat"]
ii[["turtle", "dog"]]
ii[2]
ii[2:3]
# Even better, you can define meta-names
set_group(ii, "mammals", ["cat", "dog"])
ii["mammals"]
# Here is the structure of an index, a Dict pointing to a list of names:
dump(ii)
# and this can be very useful shorthand for DFs
set_group(df1, "IVs", ["a", "v1"])
print(df1)
df1["IVs"]
# note that this expansion should work in Formulas


# rbind and cbind should eventually work like their R equivalents; hcat and vcat are
# Julian aliases. They're methods need fleshing out at the moment
cbind(df1, DataFrame(:("cat"=[5:14])))
rbind(df1, DataFrame(:("a"=99; "b"="B"; "v1"=3.1415)))

# a SubDataFrame is a lightweight wrapper around a DataFrame used most frequently in
# split/apply sorts of operations.
df1 = DataFrame(quote
    a = shuffle([1:10])
    b = ["A","B"][randi(2,10)]
    v1 = randn(10)
end)
sub(df1, [1:5])
sub(df1, [1:5], "v1")

# Here is the structure of a SubDataFrame:
idump( sub(df1, [1:5]), 2)


# Expressions are used in many places in DataFrames.jl. The functions
# `with`, `within`, and `based_on` evaluate an expression within a
# DataFrame. `with` returns the result of the evaluation. These all
# work by traversing the expression and replacing terms that are in
# the DataFrame and then eval'ing the expression.
with(df1, :( a + sum(v1) ))
# Because `eval` acts globally, it can pull in global variables but
# not local variables. To insert those, they must be escaped.
a2local = df1["a"] + df1["a"]
with(df1, :( a + $a2local))

# `within(df)` returns the a copy of `df` after evaluating the
# expression. This is most useful for creating new columns from
# existing columns. `within!` is the same but changes the original
# DataFrame.
within(df1, :( a2 = 2 + a ))

# `based_on(df)` evaluates to a new DataFrame using the contents of `df`. 
df2 = based_on(df1, quote
    a = a + 2
    v1sum = sum(v1)
end)

# Rows of DataFrames may also be selected based on the results of an
# expression, somewhat like R's data.table package.
df1[:( a .> 5 )]
# equivalent to:
df1[with(df1, :(a .> 5)), :]

# TODO: groupedDataFrame


# TODO: grouping operations



# TODO: pipelining
# The pipeline operator (`|`) is useful for many DataFrames.jl operations.
# This is handy at the REPL to evaluate the structure of a result:
df1[:( a .> 5 )] | dump




# TODO: by, colwise


# TODO: stack, unstack



