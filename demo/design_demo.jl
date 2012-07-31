
# there are some warnings here that need to be addressed...
load("src/init.jl")

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
# array is referenced, not copied. In general, the JuliaData philosophy is 
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
sum(dvint) # fails
sum(naFilter(dvint))
sum(naReplace(dvint, 100))

# A PooledDataVec is an implementation of AbstractDataVec that uses indexes into
# a pool of values instead of an array of values, which is space efficient in 
# the case of a large number of repeated elements. A common use is repeated 
# strings. PDVs don't have a mask -- they can use a 0-index to represent the NA.
pdvstr = PooledDataVec["one", "one", "two", "two", NA, "one", "one"]
print(pdvstr.pool)
print(pdvstr.refs)
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
# and this can be very useful shorthand for DFs
set_group(df1, "IVs", ["a", "v1"])
print(df1)
df1["IVs"]



