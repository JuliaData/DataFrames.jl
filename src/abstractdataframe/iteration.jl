##############################################################################
##
## Iteration: eachrow, eachcol
##
##############################################################################

# TODO: Reconsider/redesign eachrow -- ~100% overhead

# Iteration by rows
"""
    DataFrameRows{T<:AbstractDataFrame} <: AbstractVector{DataFrameRow{T}}

Iterator over rows of an `AbstractDataFrame`,
with each row represented as a `DataFrameRow`.

A value of this type is returned by the [`eachrow`](@link) function.
"""
struct DataFrameRows{T<:AbstractDataFrame} <: AbstractVector{DataFrameRow{T}}
    df::T
end

"""
    eachrow(df::AbstractDataFrame)

Return a `DataFrameRows` that iterates an `AbstractDataFrame` row by row,
with each row represented as a `DataFrameRow`.
"""
eachrow(df::AbstractDataFrame) = DataFrameRows(df)

Base.size(itr::DataFrameRows) = (size(itr.df, 1), )
Base.IndexStyle(::Type{<:DataFrameRows}) = Base.IndexLinear()
@inline function Base.getindex(itr::DataFrameRows, i::Int)
    @boundscheck checkbounds(itr, i)
    return DataFrameRow(itr.df, i)
end

# Iteration by columns
"""
    DataFrameColumns{<:AbstractDataFrame, V} <: AbstractVector{V}

Iterator over columns of an `AbstractDataFrame`.
If `V` is `Pair{Symbol,AbstractVector}` (which is the case when calling
[`eachcol`](@link)) then each returned value is a pair consisting of
column name and column vector. If `V` is `AbstractVector` (a value returned by
the [`columns`](@link) function) then each returned value is a column vector.
"""
struct DataFrameColumns{T<:AbstractDataFrame, V} <: AbstractVector{V}
    df::T
end

"""
    eachcol(df::AbstractDataFrame)

Return a `DataFrameColumns` that iterates an `AbstractDataFrame` column by column.
Iteration returns a pair consisting of column name and column vector.

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

julia> collect(eachcol(df))
2-element Array{Pair{Symbol,AbstractArray{T,1} where T},1}:
 :x => [1, 2, 3, 4]
 :y => [11, 12, 13, 14]
```
"""
eachcol(df::T) where T<: AbstractDataFrame =
    DataFrameColumns{T, Pair{Symbol, AbstractVector}}(df)

"""
    columns(df::AbstractDataFrame)

Return a `DataFrameColumns` that iterates an `AbstractDataFrame` column by
column, yielding column vectors.

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

julia> collect(columns(df))
2-element Array{AbstractArray{T,1} where T,1}:
 [1, 2, 3, 4]
 [11, 12, 13, 14]

julia> sum.(columns(df))
2-element Array{Int64,1}:
 10
 50

julia> map(columns(df)) do col
       maximum(col) - minimum(col)
       end
2-element Array{Int64,1}:
 3
 3
```
"""
columns(df::T) where T<: AbstractDataFrame =
    DataFrameColumns{T, AbstractVector}(df)

Base.size(itr::DataFrameColumns) = (size(itr.df, 2),)
Base.IndexStyle(::Type{<:DataFrameColumns}) = Base.IndexLinear()
Base.getindex(itr::DataFrameColumns{<:AbstractDataFrame,
                                    Pair{Symbol, AbstractVector}}, j::Int) =
    _names(itr.df)[j] => itr.df[j]
Base.getindex(itr::DataFrameColumns{<:AbstractDataFrame,AbstractVector}, j::Int) =
    itr.df[j]

"""
    mapcols(f::Union{Function,Type}, df::AbstractDataFrame)

Return a `DataFrame` where each column of `df` is transformed using function `f`.
Note that `f` must return values of consistent length.

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
    for (n, v) in eachcol(df)
        res[n] = f(v)
    end
    res
end
