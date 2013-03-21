##############################################################################
##
## Reshaping
##
## Also, see issue # ??
##
##############################################################################

##############################################################################
##
## stack()
## melt()
##
##############################################################################

function stack(df::DataFrame, measure_vars::Vector{Int}, id_vars::Vector{Int})
    res = [insert!(df[[i, id_vars]], 1, colnames(df)[i], "variable") for i in measure_vars]
    # fix column names
    map(x -> colnames!(x, ["variable", "value", colnames(df[id_vars])]), res)
    res = rbind(res)
    res 
end
stack(df::DataFrame, measure_vars, id_vars) = stack(df, [df.colindex[measure_vars]], [df.colindex[id_vars]])
stack(df::DataFrame, measure_vars) = stack(df, [df.colindex[measure_vars]], _setdiff([1:ncol(df)], [df.colindex[measure_vars]]))

melt(df::DataFrame, id_vars) = stack(df, _setdiff([1:ncol(df)], [df.colindex[id_vars]]))
melt(df::DataFrame, id_vars, measure_vars) = stack(df, measure_vars, id_vars)

##############################################################################
##
## unstack()
##
##############################################################################

function unstack(df::AbstractDataFrame, rowkey::Int, colkey::Int, value::Int)
    # `rowkey` integer indicating which column to place along rows
    # `colkey` integer indicating which column to place along column headers
    # `value` integer indicating which column has values
    keycol = PooledDataArray(df[rowkey])
    valuecol = df[value]
    # TODO make a version with a default refkeycol
    refkeycol = PooledDataArray(df[colkey])
    remainingcols = _setdiff([1:ncol(df)], [rowkey, value])
    Nrow = length(refkeycol.pool)
    Ncol = length(keycol.pool)
    # TODO make fillNA(type, length) 
    payload = DataFrame({DataArray([fill(valuecol[1],Nrow)], fill(true, Nrow))  for i in 1:Ncol}, map(string, keycol.pool))
    nowarning = true 
    for k in 1:nrow(df)
        j = int(keycol.refs[k])
        i = int(refkeycol.refs[k])
        if i > 0 && j > 0
            if nowarning && !isna(payload[j][i]) 
                println("Warning: duplicate entries in unstack.")
                nowarning = false
            end
            payload[j][i]  = valuecol[k]
        end
    end
    insert!(payload, 1, refkeycol.pool, colnames(df)[colkey])
end
unstack(df::AbstractDataFrame, rowkey, value, colkey) =
    unstack(df, index(df)[rowkey], index(df)[value], index(df)[colkey])

##############################################################################
##
## pivot_table()
##
##############################################################################

# Limitations:
#  - only one `value` column is allowed (same as dcast)
#  - `fun` must reduce to one value
#  - no margins
#  - can't have zero rows or zero columns
#  - the resulting data part is Float64 (`payload` below) 

function pivot_table(df::AbstractDataFrame, rows::Vector{Int}, cols::Vector{Int}, value::Int, fun::Function)
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
    payload = DataFrame(Nrow, Ncol)
    colnames!(payload, col_pdv.pool)
    for i in 1:length(row_idxs)
        payload[row_idxs[i], col_idxs[i]] = cmb_df[i,"x1"]
    end
    # find the "row" key DataFrame
    g = groupby(cmb_df[[1:length(rows)]], [1:length(rows)])
    row_key_df = g.parent[g.idx[g.starts],:]
    cbind(row_key_df, payload)
end
# `mean` is the default aggregation function:
pivot_table(df::AbstractDataFrame, rows, cols, value) = pivot_table(df, rows, cols, value, mean)
pivot_table(df::AbstractDataFrame, rows, cols, value, fun) = pivot_table(df, [index(df)[rows]], [index(df)[cols]], index(df)[value], fun)

    
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

function getindex(v::StackedVector,i::Real)
    lengths = [length(x)::Int for x in v.components]
    cumlengths = [0, cumsum(lengths)]
    j = searchsortedlast(cumlengths + 1, i)
    if j > length(cumlengths)
        error("indexing bounds error")
    end
    k = i - cumlengths[j]
    if k < 1 || k > length(v.components[j])
        error("indexing bounds error")
    end
    v.components[j][k]
end

function getindex{I<:Real}(v::StackedVector,i::AbstractVector{I})
    result = similar(v.components[1], length(i))
    for idx in 1:length(i)
        result[idx] = v[i[idx]]
    end
    result
end
getindex(v::StackedVector,i::Union(Ranges, Vector{Bool}, BitVector)) = getindex(v, [i])

size(v::StackedVector) = (length(v),)
length(v::StackedVector) = sum(map(length, v.components))
ndims(v::StackedVector) = 1
eltype(v::StackedVector) = promote_type(map(eltype, v.components)...)
vecbind_type(v::StackedVector) = vecbind_promote_type(map(vecbind_type, v.components)...)
similar(v::StackedVector, T, dims::Dims) = similar(v.components[1], T, dims)

show(io::IO, v::StackedVector) = internal_show_vector(io, v)
repl_show(io::IO, v::StackedVector) = internal_repl_show_vector(io, v)

PooledDataArray(v::StackedVector) = PooledDataArray(v[:]) # could be more efficient

function getindex{T,I<:Real}(v::RepeatedVector{T},i::AbstractVector{I})
    j = mod(i - 1, length(v.parent)) + 1
    v.parent[j]
end
function getindex{T}(v::RepeatedVector{T},i::Real)
    j = mod(i - 1, length(v.parent)) + 1
    v.parent[j]
end
getindex(v::RepeatedVector,i::Union(Ranges, Vector{Bool}, BitVector)) = getindex(v, [i])

size(v::RepeatedVector) = (length(v),)
length(v::RepeatedVector) = v.n * length(v.parent)
ndims(v::RepeatedVector) = 1
eltype{T}(v::RepeatedVector{T}) = T
vecbind_type(v::RepeatedVector) = vecbind_type(v.parent)
reverse(v::RepeatedVector) = RepeatedVector(reverse(v.parent), v.n)
similar(v::RepeatedVector, T, dims::Dims) = similar(v.parent, T, dims)

show(io::IO, v::RepeatedVector) = internal_show_vector(io, v)
repl_show(io::IO, v::RepeatedVector) = internal_repl_show_vector(io, v)

unique(v::RepeatedVector) = unique(v.parent)

function PooledDataArray(v::RepeatedVector)
    res = PooledDataArray(v.parent)
    res.refs = rep(res.refs, v.n)
    res
end

function getindex{T}(v::EachRepeatedVector{T},i::Real)
    j = div(i - 1, v.n) + 1
    v.parent[j]
end
function getindex{T,I<:Real}(v::EachRepeatedVector{T},i::AbstractVector{I})
    j = div(i - 1, v.n) + 1
    v.parent[j]
end
getindex(v::EachRepeatedVector,i::Union(Ranges, Vector{Bool}, BitVector)) = getindex(v, [i])

size(v::EachRepeatedVector) = (length(v),)
length(v::EachRepeatedVector) = v.n * length(v.parent)
ndims(v::EachRepeatedVector) = 1
eltype{T}(v::EachRepeatedVector{T}) = T
vecbind_type(v::EachRepeatedVector) = vecbind_type(v.parent)
reverse(v::EachRepeatedVector) = EachRepeatedVector(reverse(v.parent), v.n)
similar(v::EachRepeatedVector, T, dims::Dims) = similar(v.parent, T, dims)

show(io::IO, v::EachRepeatedVector) = internal_show_vector(io, v)
repl_show(io::IO, v::EachRepeatedVector) = internal_repl_show_vector(io, v)

unique(v::EachRepeatedVector) = unique(v.parent)

PooledDataArray(v::EachRepeatedVector) = PooledDataArray(v[:], removeNA(unique(v.parent)))

function PooledDataArray(v::EachRepeatedVector)
    res = PooledDataArray(v.parent)
    res.refs = rep(res.refs, rep(v.n,length(res.refs)))
    res
end


# The default values of show and repl_show don't work because
# both try to reshape the vector into a matrix, and these
# types do not support that.

function internal_show_vector(io::IO, v::AbstractVector)
    Base.show_delim_array(io, v, '[', ',', ']', true)
end

function internal_repl_show_vector(io::IO, v::AbstractVector)
    print(io, summary(v))
    if length(v) < 21
        print_matrix(io, v[:]'')    # the double transpose ('') reshapes to a matrix
    else
        println(io)
        print_matrix(io, v[1:10]'')
        println(io)
        println(io, "  \u22ee")
        print_matrix(io, v[end - 9:end]'')
    end
end


##############################################################################
##
## stack_df()
## melt_df()
## Reshaping using referencing (issue #145), using the above vector types
##
##############################################################################

# Same as `stack`, but uses references
# I'm not sure the name is very good
function stack_df(df::AbstractDataFrame, measure_vars::Vector{Int}, id_vars::Vector{Int})
    N = length(measure_vars)
    remainingcols = _setdiff([1:ncol(df)], measure_vars)
    names = colnames(df)[id_vars]
    insert!(names, 1, "value")
    insert!(names, 1, "variable")
    DataFrame({EachRepeatedVector(colnames(df)[measure_vars], nrow(df)), # variable
               StackedVector({df[:,c] for c in 1:N}),                    # value
               ## RepeatedVector([1:nrow(df)], N),                       # idx - do we want this?
               [RepeatedVector(df[:,c], N) for c in id_vars]...},        # id_var columns
              names)
end
stack_df(df::AbstractDataFrame, measure_vars) = stack_df(df, [index(df)[measure_vars]])

stack_df(df::AbstractDataFrame, measure_vars, id_vars) = stack_df(df, [index(df)[measure_vars]], [index(df)[id_vars]])
stack_df(df::AbstractDataFrame, measure_vars) = stack_df(df, [index(df)[measure_vars]], _setdiff([1:ncol(df)], [index(df)[measure_vars]]))

melt_df(df::AbstractDataFrame, id_vars) = stack_df(df, _setdiff([1:ncol(df)], index(df)[id_vars]))
melt_df(df::AbstractDataFrame, id_vars, measure_vars) = stack_df(df, measure_vars, id_vars)
