##############################################################################
##
## Iteration: eachrow, eachcol
##
##############################################################################

# TODO: Reconsider/redesign eachrow -- ~100% overhead

# Iteration by rows
"""
    DFRowVector{T<:AbstractDataFrame} <: AbstractVector{DataFrameRow{T}}

Iterator over rows of an `AbstractDataFrame`,
with each row represented as a `DataFrameRow`.

A value of this type is returned by the [`eachrow`](@link) function.
"""
struct DFRowVector{T<:AbstractDataFrame} <: AbstractVector{DataFrameRow{T}}
    df::T
end

"""
    eachrow(df::AbstractDataFrame)

Return a `DFRowVector` that iterates an `AbstractDataFrame` row by row,
with each row represented as a `DataFrameRow`.
"""
eachrow(df::AbstractDataFrame) = DFRowVector(df)

Base.size(itr::DFRowVector) = (size(itr.df, 1), )
Base.IndexStyle(::Type{<:DFRowVector}) = Base.IndexLinear()
Base.getindex(itr::DFRowVector, i::Int) = DataFrameRow(itr.df, i)

# Iteration by columns
"""
    DFColumnVector{<:AbstractDataFrame, V} <: AbstractVector{V}

Iterator over columns of an `AbstractDataFrame`.
If `V` is `Pair{Symbol,AbstractVector}` (which is the case when calling
[`eachcol`](@link)) then each returned value is a pair consisting of
column name and column vector. If `V` is `AbstractVector` (a value returned by
the [`columns`](@link) function) then each returned value is a column vector.
"""
struct DFColumnVector{T<:AbstractDataFrame, V} <: AbstractVector{V}
    df::T
end

"""
    eachcol(df::AbstractDataFrame)

Return a `DFColumnVector` that iterates an `AbstractDataFrame` column by column.
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
    DFColumnVector{T, Pair{Symbol, AbstractVector}}(df)

"""
    columns(df::AbstractDataFrame)

Return a `DFColumnVector` that iterates an `AbstractDataFrame` column by
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
```
"""
columns(df::T) where T<: AbstractDataFrame =
    DFColumnVector{T, AbstractVector}(df)

Base.size(itr::DFColumnVector) = (size(itr.df, 2),)
Base.IndexStyle(::Type{<:DFColumnVector}) = Base.IndexLinear()
Base.getindex(itr::DFColumnVector{<:AbstractDataFrame,
                                    Pair{Symbol, AbstractVector}}, j::Int) =
    _names(itr.df)[j] => itr.df[j]
Base.getindex(itr::DFColumnVector{<:AbstractDataFrame,AbstractVector}, j::Int) =
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
