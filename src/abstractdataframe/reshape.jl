##############################################################################
##
## Reshaping
##
## Also, see issue # ??
##
##############################################################################

typealias Ints Union(Int, Vector{Int})

##############################################################################
##
## stack()
## melt()
##
##############################################################################

function stack(df::AbstractDataFrame, measure_vars::Ints, id_vars::Ints)
    res = DataFrame[insert!(df[[i, id_vars]], 1, names(df)[i], :variable) for i in measure_vars]
    # fix column names
    map(x -> names!(x, [:variable, :value, names(df[id_vars])]), res)
    vcat(res)
end
stack(df::AbstractDataFrame, measure_vars, id_vars) =
    stack(df, index(df)[measure_vars], index(df)[id_vars])
function stack(df::AbstractDataFrame, measure_vars)
    mv_inds = index(df)[measure_vars]
    stack(df, mv_inds, _setdiff(1:ncol(df), mv_inds))
end

function melt(df::AbstractDataFrame, id_vars)
    id_inds = index(df)[id_vars]
    stack(df, _setdiff(1:ncol(df), id_inds), id_inds)
end
melt(df::AbstractDataFrame, id_vars, measure_vars) = stack(df, measure_vars, id_vars)

##############################################################################
##
## unstack()
##
##############################################################################

function unstack(df::AbstractDataFrame, rowkey::Int, colkey::Int, value::Int)
    # `rowkey` integer indicating which column to place along rows
    # `colkey` integer indicating which column to place along column headers
    # `value` integer indicating which column has values
    refkeycol = PooledDataArray(df[rowkey])
    valuecol = df[value]
    # TODO make a version with a default refkeycol
    keycol = PooledDataArray(df[colkey])
    Nrow = length(refkeycol.pool)
    Ncol = length(keycol.pool)
    # TODO make fillNA(type, length)
    payload = DataFrame(Any[DataArray([fill(valuecol[1], Nrow)], fill(true, Nrow)) for i in 1:Ncol], map(symbol, keycol.pool))
    nowarning = true
    for k in 1:nrow(df)
        j = int(keycol.refs[k])
        i = int(refkeycol.refs[k])
        if i > 0 && j > 0
            if nowarning && !isna(payload[j][i])
                warn("Duplicate entries in unstack.")
                nowarning = false
            end
            payload[j][i]  = valuecol[k]
        end
    end
    insert!(payload, 1, refkeycol.pool, names(df)[colkey])
end
unstack(df::AbstractDataFrame, rowkey, colkey, value) =
    unstack(df, index(df)[rowkey], index(df)[colkey], index(df)[value])

##############################################################################
##
## pivottable()
##
##############################################################################

# Limitations:
#  - only one `value` column is allowed (same as dcast)
#  - `fun` must reduce to one value
#  - no margins
#  - can't have zero rows or zero columns
#  - the resulting data part is Float64 (`payload` below)

function pivottable(df::AbstractDataFrame, rows::Ints, cols::Ints, value::Int, fun::Function)
    # `rows` vector indicating which columns are keys placed in rows
    # `cols` vector indicating which columns are keys placed as column headers
    # `value` integer indicating which column has values
    # `fun` function applied to the value column during aggregation
    cmb_df = by(df, [rows, cols], d->fun(d[value]))
    row_pdv = PooledDataArray(paste_columns(cmb_df[[length(rows):-1:1]]))  # the :-1: is to reverse the columns for sorting
    row_idxs = int(row_pdv.refs)
    Nrow = length(row_pdv.pool)
    col_pdv = PooledDataArray(paste_columns(cmb_df[[length(rows) + (1:length(cols))]]))
    col_idxs = int(col_pdv.refs)
    Ncol = length(col_pdv.pool)
    # `payload` is the main "data holding" part of the resulting DataFrame
    payload = DataFrame(Float64, Nrow, Ncol)
    names!(payload, convert(Vector{Symbol}, col_pdv.pool))
    for i in 1:length(row_idxs)
        payload[row_idxs[i], col_idxs[i]] = cmb_df[i, :x1]
    end
    # find the "row" key DataFrame
    g = groupby(cmb_df[1:length(rows)], 1:length(rows))
    row_key_df = g.parent[g.idx[g.starts], :]
    hcat!(row_key_df, payload)
end
# `mean` is the default aggregation function:
pivottable(df::AbstractDataFrame, rows, cols, value) = pivottable(df, rows, cols, value, mean)
pivottable(df::AbstractDataFrame, rows, cols, value, fun) = pivottable(df, index(df)[rows], index(df)[cols], index(df)[value], fun)
pivottable(fun::Function, df::AbstractDataFrame, rows, cols, value) = pivottable(df, rows, cols, value, fun)

function paste_columns(df::AbstractDataFrame, sep)
    res = [string(v) for v in df[1]]
    for j in 2:ncol(df), i in 1:nrow(df)
        res[i] *= string(sep, df[i, j])
    end
    res
end
paste_columns(df::AbstractDataFrame) = paste_columns(df, "_")

##############################################################################
##
## Reshaping using referencing (issue #145)
## New AbstractVector types (all read only):
##     StackedVector
##     RepeatedVector
##     EachRepeatedVector
##
##############################################################################

## StackedVector({[1,2], [9,10], [11,12]}) is equivalent to [1,2,9,10,11,12]
type StackedVector <: AbstractVector{Any}
    components::Vector{Any}
end

## RepeatedVector([1,2], 3) is equivalent to [1,2,1,2,1,2]
type RepeatedVector{T} <: AbstractVector{T}
    parent::AbstractVector{T}
    n::Int
end

## EachRepeatedVector([1,2], 3) is equivalent to [1,1,1,2,2,2]
type EachRepeatedVector{T} <: AbstractVector{T}
    parent::AbstractVector{T}
    n::Int
end

function Base.getindex(v::StackedVector,i::Real)
    lengths = [length(x)::Int for x in v.components]
    cumlengths = [0, cumsum(lengths)]
    j = searchsortedlast(cumlengths .+ 1, i)
    if j > length(cumlengths)
        error("indexing bounds error")
    end
    k = i - cumlengths[j]
    if k < 1 || k > length(v.components[j])
        error("indexing bounds error")
    end
    v.components[j][k]
end

function Base.getindex{I<:Real}(v::StackedVector,i::AbstractVector{I})
    result = similar(v.components[1], length(i))
    for idx in 1:length(i)
        result[idx] = v[i[idx]]
    end
    result
end
Base.getindex(v::StackedVector,i::Union(Ranges, Vector{Bool}, BitVector)) = getindex(v, [i])

Base.size(v::StackedVector) = (length(v),)
Base.length(v::StackedVector) = sum(map(length, v.components))
Base.ndims(v::StackedVector) = 1
Base.eltype(v::StackedVector) = promote_type(map(eltype, v.components)...)
Base.similar(v::StackedVector, T, dims::Dims) = similar(v.components[1], T, dims)

DataArrays.PooledDataArray(v::StackedVector) = PooledDataArray(v[:]) # could be more efficient

function Base.getindex{T,I<:Real}(v::RepeatedVector{T},i::AbstractVector{I})
    j = mod(i .- 1, length(v.parent)) .+ 1
    v.parent[j]
end
function Base.getindex{T}(v::RepeatedVector{T},i::Real)
    j = mod(i - 1, length(v.parent)) + 1
    v.parent[j]
end
Base.getindex(v::RepeatedVector,i::Union(Ranges, Vector{Bool}, BitVector)) = getindex(v, [i])

Base.size(v::RepeatedVector) = (length(v),)
Base.length(v::RepeatedVector) = v.n * length(v.parent)
Base.ndims(v::RepeatedVector) = 1
Base.eltype{T}(v::RepeatedVector{T}) = T
Base.reverse(v::RepeatedVector) = RepeatedVector(reverse(v.parent), v.n)
Base.similar(v::RepeatedVector, T, dims::Dims) = similar(v.parent, T, dims)
Base.unique(v::RepeatedVector) = unique(v.parent)

function DataArrays.PooledDataArray(v::RepeatedVector)
    res = PooledDataArray(v.parent)
    res.refs = rep(res.refs, v.n)
    res
end

function Base.getindex{T}(v::EachRepeatedVector{T},i::Real)
    j = div(i - 1, v.n) + 1
    v.parent[j]
end
function Base.getindex{T,I<:Real}(v::EachRepeatedVector{T},i::AbstractVector{I})
    j = div(i .- 1, v.n) .+ 1
    v.parent[j]
end
Base.getindex(v::EachRepeatedVector,i::Union(Ranges, Vector{Bool}, BitVector)) = getindex(v, [i])

Base.size(v::EachRepeatedVector) = (length(v),)
Base.length(v::EachRepeatedVector) = v.n * length(v.parent)
Base.ndims(v::EachRepeatedVector) = 1
Base.eltype{T}(v::EachRepeatedVector{T}) = T
Base.reverse(v::EachRepeatedVector) = EachRepeatedVector(reverse(v.parent), v.n)
Base.similar(v::EachRepeatedVector, T, dims::Dims) = similar(v.parent, T, dims)
Base.unique(v::EachRepeatedVector) = unique(v.parent)

DataArrays.PooledDataArray(v::EachRepeatedVector) = PooledDataArray(v[:], dropna(unique(v.parent)))

function DataArrays.PooledDataArray(v::EachRepeatedVector)
    res = PooledDataArray(v.parent)
    res.refs = rep(res.refs, rep(v.n,length(res.refs)))
    res
end

##############################################################################
##
## stackdf()
## meltdf()
## Reshaping using referencing (issue #145), using the above vector types
##
##############################################################################

# Same as `stack`, but uses references
# I'm not sure the name is very good
function stackdf(df::AbstractDataFrame, measure_vars::Vector{Int}, id_vars::Ints)
    N = length(measure_vars)
    cnames = names(df)[id_vars]
    insert!(cnames, 1, "value")
    insert!(cnames, 1, "variable")
    DataFrame(Any[EachRepeatedVector(names(df)[measure_vars], nrow(df)), # variable
                  StackedVector(Any[df[:,c] for c in measure_vars]),     # value
               ## RepeatedVector([1:nrow(df)], N),                       # idx - do we want this?
                  [RepeatedVector(df[:,c], N) for c in id_vars]...],     # id_var columns
              cnames)
end
function stackdf(df::AbstractDataFrame, measure_vars::Int, id_vars)
    stackdf(df, [measure_vars], id_vars)
end
function stackdf(df::AbstractDataFrame, measure_vars, id_vars)
    stackdf(df, index(df)[measure_vars], index(df)[id_vars])
end
function stackdf(df::AbstractDataFrame, measure_vars)
    m_inds = index(df)[measure_vars]
    stackdf(df, m_inds, _setdiff(1:ncol(df), m_inds))
end

function meltdf(df::AbstractDataFrame, id_vars)
    id_inds = index(df)[id_vars]
    stackdf(df, _setdiff(1:ncol(df), id_inds), id_inds)
end
meltdf(df::AbstractDataFrame, id_vars, measure_vars) =
    stackdf(df, measure_vars, id_vars)
