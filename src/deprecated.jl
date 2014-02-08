using Base.depwarn

Base.@deprecate vecbind vcat
Base.@deprecate rbind vcat
Base.@deprecate cbind hcat
Base.@deprecate read_table readtable
Base.@deprecate print_table printtable
Base.@deprecate write_table writetable
Base.@deprecate merge(df1::AbstractDataFrame, df2::AbstractDataFrame; on::Any = nothing, kind::Symbol = :inner) Base.join
Base.@deprecate colnames(adf::AbstractDataFrame) Base.names
Base.@deprecate colnames!(adf::AbstractDataFrame, vals) names!
Base.@deprecate coltypes(adf::AbstractDataFrame) types
Base.@deprecate EachRow eachrow
Base.@deprecate EachCol eachcol
Base.@deprecate subset sub

function DataFrame(df::DataFrame)
    depwarn("DataFrame(::DataFrame) is deprecated, use convert(DataFrame, DataFrame) instead",
            :DataFrame)
    return df
end

function DataFrame(x::Union(Number, String))
    depwarn("DataFrame(::Union(Number, String)) is deprecated, use DataFrame(Vector{Any}) instead",
            :DataFrame)
    cols = {DataArray([x], falses(1))}
    colind = Index(gennames(1))
    return DataFrame(cols, colind)
end

# TODO: Replace this with convert call.
# Convert a standard Matrix to a DataFrame w/ pre-specified names
function DataFrame(x::Matrix, cn::Vector = gennames(size(x, 2)))
    depwarn("DataFrame(::Matrix, ::Vector)) is deprecated, use convert(DataFrame, Matrix) instead",
            :DataFrame)
    n = length(cn)
    cols = Array(Any, n)
    for i in 1:n
        cols[i] = DataArray(x[:, i])
    end
    return DataFrame(cols, Index(cn))
end

function DataFrame{T<:String}(columns::Vector{Any}, cnames::Vector{T})
    depwarn("DataFrame(::Vector{Any}, ::Vector{T<:String}) is deprecated, use DataFrame(::Vector{Any}, ::Vector{Symbol}) instead",
            :DataFrame)
    DataFrame(columns, map(symbol, cnames))
end

function DataFrame{D <: Associative, T <: String}(ds::Vector{D}, ks::Vector{T})
    depwarn("DataFrame(::Vector{D<:Associative}, ::Vector{T<:String}) is deprecated, use DataFrame(::Vector{D<:Associative}, ::Vector{Symbol}) instead",
            :DataFrame)
    DataFrame(ds, map(symbol, ks))
end

# Iteration matches that of Associative types (experimental)
function Base.start(df::AbstractDataFrame)
    depwarn("Default AbstractDataFrame iterator is deprecated, use eachcol(::AbstractDataFrame) instead",
            :AbstractDataFrame)
    1
end
Base.done(df::AbstractDataFrame, i) = i > ncol(df)
Base.next(df::AbstractDataFrame, i) = ((names(df)[i], df[i]), i + 1)

##############################################################################
##
## Dict conversion
##
## Try to insure this invertible.
## Allow option to flatten a single row.
##
##############################################################################

function dict(adf::AbstractDataFrame, flatten::Bool = false)
    depwarn("dict(::AbstractDataFrame, ::Bool) is deprecated", :dict)
    res = Dict{Symbol, Any}()
    if flatten && size(adf, 1) == 1
        for colname in names(adf)
            res[colname] = adf[colname][1]
        end
    else
        for colname in names(adf)
            res[colname] = adf[colname]
        end
    end
    return res
end

function pool!(df::AbstractDataFrame, cname::String)
    depwarn("pool!(::AbstractDataFrame, ::String) is deprecated, use pool!(::AbstractDataFrame, ::Symbol) instead", :pool!)
    pool!(df, symbol(cname))
end

function pool!{T<:String}(df::AbstractDataFrame, cname::Vector{T})
    depwarn("pool!(::AbstractDataFrame, ::Vector{T<:String}) is deprecated, use pool!(::AbstractDataFrame, ::Vector{T<:Symbol}) instead", :pool!)
    pool!(df, map(symbol, cnames))
end

function Base.getindex(df::DataFrame, col_ind::String)
    depwarn("indexing DataFrames with strings is deprecated; use symbols instead", :getindex)
    getindex(df, symbol(col_ind))
end

function Base.getindex{T<:String}(df::DataFrame, col_inds::AbstractVector{T})
    depwarn("indexing DataFrames with strings is deprecated; use symbols instead", :getindex)
    getindex(df, map(symbol, col_inds))
end

function Base.getindex(df::DataFrame, row_ind, col_ind::String)
    depwarn("indexing DataFrames with strings is deprecated; use symbols instead", :getindex)
    getindex(df, row_ind, symbol(col_ind))
end

function Base.getindex{T<:String}(df::DataFrame, row_ind, col_inds::AbstractVector{T})
    depwarn("indexing DataFrames with strings is deprecated; use symbols instead", :getindex)
    getindex(df, row_ind, map(symbol, col_inds))
end

function Base.getindex(x::AbstractIndex, idx::String)
    depwarn("indexing DataFrames with strings is deprecated; use symbols instead", :getindex)
    getindex(x, symbol(idx))
end

function Base.getindex{T<:String}(x::AbstractIndex, idx::AbstractVector{T})
    depwarn("indexing DataFrames with strings is deprecated; use symbols instead", :getindex)
    getindex(x, map(symbol, idx))
end

function Base.setindex!(df::DataFrame, v, col_ind::String)
    depwarn("indexing DataFrames with strings is deprecated; use symbols instead", :setindex!)
    setindex!(df, v, symbol(col_ind))
end

function Base.setindex!{T<:String}(df::DataFrame, v, col_inds::AbstractVector{T})
    depwarn("indexing DataFrames with strings is deprecated; use symbols instead", :setindex!)
    setindex!(df, v, map(symbol, col_ind))
end

function Base.setindex!(df::DataFrame, v, row_ind, col_ind::String)
    depwarn("indexing DataFrames with strings is deprecated; use symbols instead", :setindex!)
    setindex!(df, v, row_ind, symbol(col_ind))
end

function Base.assign{T<:String}(df::DataFrame, v, row_ind, col_inds::AbstractVector{T})
    depwarn("indexing DataFrames with strings is deprecated; use symbols instead", :setindex!)
    setindex!(df, v, row_ind, map(symbol, col_ind))
end


# reorder! for factors by specifying a DataFrame
function DataArrays.reorder(fun::Function, x::PooledDataArray, df::AbstractDataFrame)
    depwarn("reordering DataFrames is deprecated", :reorder)
    dfc = copy(df)
    dfc["__key__"] = x
    gd = by(dfc, "__key__", df -> colwise(fun, without(df, "__key__")))
    idx = sortperm(gd[[2:ncol(gd)]])
    return PooledDataArray(x, dropna(gd[idx,1]))
end
function DataArrays.reorder(x::PooledDataArray, df::AbstractDataFrame)
    depwarn("reordering DataFrames is deprecated", :reorder)
    reorder(:mean, x, df)
end

function DataArrays.reorder(fun::Function, x::PooledDataArray, y::AbstractVector...)
    depwarn("reordering DataFrames is deprecated", :reorder)
    reorder(fun, x, DataFrame({y...}))
end

function Base.flipud(df::DataFrame)
    depwarn("flipud(DataFrame)", :flipud)
    return df[reverse(1:nrow(df)), :]
end

function flipud!(df::DataFrame)
    depwarn("flipud!(DataFrame)", :flipud!)
    df[1:nrow(df), :] = df[reverse(1:nrow(df)), :]
    return
end

function cleannames!(df::DataFrame)
    depwarn("cleannames!(DataFrame)", :cleannames!)
    oldnames = map(strip, names(df))
    newnames = map(n -> replace(n, r"\W", "_"), oldnames)
    names!(df, newnames)
    return
end

