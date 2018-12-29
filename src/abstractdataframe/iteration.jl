##############################################################################
##
## Iteration: eachrow, eachcol
##
##############################################################################

# TODO: Reconsider/redesign eachrow -- ~100% overhead

# Iteration by rows
"""
    DataFrameRows{D<:AbstractDataFrame,S<:AbstractIndex} <: AbstractVector{DataFrameRow{D,S}}

Iterator over rows of an `AbstractDataFrame`,
with each row represented as a `DataFrameRow`.

A value of this type is returned by the [`eachrow`](@ref) function.
"""
struct DataFrameRows{D<:AbstractDataFrame,S<:AbstractIndex} <: AbstractVector{DataFrameRow{D,S}}
    df::D
    index::S
end

Base.summary(dfrs::DataFrameRows) = "$(length(dfrs))-element DataFrameRows"
Base.summary(io::IO, dfrs::DataFrameRows) = print(io, summary(dfrs))

"""
    eachrow(df::AbstractDataFrame)

Return a `DataFrameRows` that iterates a data frame row by row,
with each row represented as a `DataFrameRow`.

**Examples**

```jldoctest
julia> df = DataFrame(x=1:4, y=11:14)
4×2 DataFrame
│ Row │ x     │ y     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 11    │
│ 2   │ 2     │ 12    │
│ 3   │ 3     │ 13    │
│ 4   │ 4     │ 14    │

julia> eachrow(df)
4-element DataFrameRows:
 DataFrameRow (row 1)
x  1
y  11
 DataFrameRow (row 2)
x  2
y  12
 DataFrameRow (row 3)
x  3
y  13
 DataFrameRow (row 4)
x  4
y  14

julia> copy.(eachrow(df))
4-element Array{NamedTuple{(:x, :y),Tuple{Int64,Int64}},1}:
 (x = 1, y = 11)
 (x = 2, y = 12)
 (x = 3, y = 13)
 (x = 4, y = 14)

julia> eachrow(view(df, [4,3], [2,1]))
2-element DataFrameRows:
 DataFrameRow (row 4)
y  14
x  4
 DataFrameRow (row 3)
y  13
x  3
```
"""
eachrow(df::AbstractDataFrame) = DataFrameRows(df, index(df))

Base.IndexStyle(::Type{<:DataFrameRows}) = Base.IndexLinear()
Base.size(itr::DataFrameRows) = (size(itr.df, 1), )

Base.@propagate_inbounds Base.getindex(itr::DataFrameRows, i::Int) =
    DataFrameRow(itr.df, itr.index, i)
Base.@propagate_inbounds Base.getindex(itr::DataFrameRows{<:SubDataFrame}, i::Int) =
    DataFrameRow(parent(itr.df), itr.index, rows(itr.df)[i])

# Iteration by columns
"""
    DataFrameColumns{<:AbstractDataFrame, V} <: AbstractVector{V}

Iterator over columns of an `AbstractDataFrame` constructed using
[`eachcol(df, true)`](@ref) if `V` is a `Pair{Symbol,AbstractVector}`. Then each
returned value is a pair consisting of column name and column vector.
If `V` is an `AbstractVector` (a value returned by [`eachcol(df, false)`](@ref))
then each returned value is a column vector.
"""
struct DataFrameColumns{T<:AbstractDataFrame, V} <: AbstractVector{V}
    df::T
end

"""
    eachcol(df::AbstractDataFrame, names::Bool=true)

Return a `DataFrameColumns` that iterates an `AbstractDataFrame` column by column.
If `names` is equal to `true` (currently the default, in the future the default
will be set to `false`) iteration returns a pair consisting of column name
and column vector.
If `names` is equal to `false` then column vectors are yielded.

**Examples**

```jldoctest
julia> df = DataFrame(x=1:4, y=11:14)
4×2 DataFrame
│ Row │ x     │ y     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 11    │
│ 2   │ 2     │ 12    │
│ 3   │ 3     │ 13    │
│ 4   │ 4     │ 14    │

julia> collect(eachcol(df, true))
2-element Array{Pair{Symbol,AbstractArray{T,1} where T},1}:
 :x => [1, 2, 3, 4]
 :y => [11, 12, 13, 14]

julia> collect(eachcol(df, false))
2-element Array{AbstractArray{T,1} where T,1}:
 [1, 2, 3, 4]
 [11, 12, 13, 14]

julia> sum.(eachcol(df, false))
2-element Array{Int64,1}:
 10
 50

julia> map(eachcol(df, false)) do col
           maximum(col) - minimum(col)
       end
2-element Array{Int64,1}:
 3
 3
```
"""
@inline function eachcol(df::T, names::Bool) where T<: AbstractDataFrame
    if names
        DataFrameColumns{T, Pair{Symbol, AbstractVector}}(df)
    else
        DataFrameColumns{T, AbstractVector}(df)
    end
end

# TODO: remove this method after deprecation
# and add default argument value above
function eachcol(df::AbstractDataFrame)
    Base.depwarn("In the future eachcol will have names argument set to false by default", :eachcol)
    eachcol(df, true)
end

# TODO: remove this method after deprecation
# this is left to make sure we do not forget to properly fix columns calls
columns(df::AbstractDataFrame) = eachcol(df, false)

Base.size(itr::DataFrameColumns) = (size(itr.df, 2),)
Base.IndexStyle(::Type{<:DataFrameColumns}) = Base.IndexLinear()

@inline function Base.getindex(itr::DataFrameColumns{<:AbstractDataFrame,
                                                     Pair{Symbol, AbstractVector}},
                               j::Int)
    @boundscheck checkbounds(itr, j)
    _names(itr.df)[j] => itr.df[j]
end

@inline function Base.getindex(itr::DataFrameColumns{<:AbstractDataFrame, AbstractVector},
                               j::Int)
    @boundscheck checkbounds(itr, j)
    itr.df[j]
end

"""
    mapcols(f::Union{Function,Type}, df::AbstractDataFrame)

Return a `DataFrame` where each column of `df` is transformed using function `f`.
`f` must return `AbstractVector` objects all with the same length or scalars.

**Examples**

```jldoctest
julia> df = DataFrame(x=1:4, y=11:14)
4×2 DataFrame
│ Row │ x     │ y     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 11    │
│ 2   │ 2     │ 12    │
│ 3   │ 3     │ 13    │
│ 4   │ 4     │ 14    │

julia> mapcols(x -> x.^2, df)
4×2 DataFrame
│ Row │ x     │ y     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 121   │
│ 2   │ 4     │ 144   │
│ 3   │ 9     │ 169   │
│ 4   │ 16    │ 196   │
```
"""
function mapcols(f::Union{Function,Type}, df::AbstractDataFrame)
    # note: `f` must return a consistent length
    res = DataFrame()
    for (n, v) in eachcol(df, true)
        res[n] = f(v)
    end
    res
end
