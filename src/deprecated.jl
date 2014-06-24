using Base.depwarn

Base.@deprecate vecbind vcat
Base.@deprecate rbind vcat
Base.@deprecate cbind hcat
Base.@deprecate read_table readtable
Base.@deprecate print_table printtable
Base.@deprecate write_table writetable
import Base.merge
Base.@deprecate merge(df1::AbstractDataFrame, df2::AbstractDataFrame; on::Any = nothing, kind::Symbol = :inner) Base.join
Base.@deprecate colnames(adf::AbstractDataFrame) Base.names
Base.@deprecate colnames!(adf::AbstractDataFrame, vals) names!
Base.@deprecate coltypes(adf::AbstractDataFrame) eltypes
Base.@deprecate types(adf::AbstractDataFrame) eltypes(adf::AbstractDataFrame)
Base.@deprecate EachRow eachrow
Base.@deprecate EachCol eachcol
Base.@deprecate subset sub
Base.@deprecate drop_duplicates! unique!
Base.@deprecate duplicated nonunique
Base.@deprecate load_df loaddf
Base.@deprecate melt_df meltdf
Base.@deprecate stack_df stackdf
Base.@deprecate pivot_table pivottable


const DEFAULT_COLUMN_ELTYPE = Float64
function DataFrame(nrows::Integer, ncols::Integer)
    depwarn("DataFrame(::Integer, ::Integer) is deprecated", :DataFrame)
    columns = Array(Any, ncols)
    for i in 1:ncols
        columns[i] = DataArray(DEFAULT_COLUMN_ELTYPE, nrows)
    end
    cnames = gennames(ncols)
    return DataFrame(columns, Index(cnames))
end

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

function DataFrame{K, V}(d::Associative{K, V})
    depwarn("DataFrame(::Vector{Associative}) with broadcasting is deprecated, please remove any reliance on broadcasting",
            :DataFrame)
    # Find the first position with maximum length in the Dict.
    lengths = map(length, values(d))
    max_length = maximum(lengths)
    maxpos = findfirst(lengths .== max_length)
    keymaxlen = keys(d)[maxpos]
    nrows = max_length
    # Start with a blank DataFrame
    df = DataFrame()
    for (k, v) in d
        if length(v) == nrows
            df[k] = v
        elseif rem(nrows, length(v)) == 0    # nrows is a multiple of length(v)
            df[k] = vcat(fill(v, div(nrows, length(v)))...)
        else
            vec = fill(v[1], nrows)
            j = 1
            for i = 1:nrows
                vec[i] = v[j]
                j += 1
                if j > length(v)
                    j = 1
                end
            end
            df[k] = vec
        end
    end
    df
end

# If we have a tuple, convert each value in the tuple to a
# DataVector and then pass the converted columns in, hoping for the best
function DataFrame(vals::Any...)
    depwarn("DataFrame(::Any...) is deprecated",
            :DataFrame)
    p = length(vals)
    columns = Array(Any, p)
    for j in 1:p
        if isa(vals[j], AbstractDataVector)
            columns[j] = vals[j]
        else
            columns[j] = convert(DataArray, vals[j])
        end
    end
    cnames = gennames(p)
    DataFrame(columns, Index(cnames))
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

function DataFrame(d::Dict)
    depwarn("DataFrame(::Dict) is deprecated, use convert(::DataFrame,d::Dict)",:convert)
    convert(DataFrame,d)
end

function DataFrame(d::Dict,cnames::Vector)
    depwarn("DataFrame(::Dict,cnames::Vector) is deprecated, use convert(::DataFrame,d::Dict)",:convert)
    convert(DataFrame,d)
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
    depwarn("flipud(DataFrame) is deprecated", :flipud)
    return df[reverse(1:nrow(df)), :]
end

function flipud!(df::DataFrame)
    depwarn("flipud!(DataFrame) is deprecated", :flipud!)
    df[1:nrow(df), :] = df[reverse(1:nrow(df)), :]
    return
end

function cleannames!(df::DataFrame)
    depwarn("cleannames!(::DataFrame) is deprecated (names are now required to be valid identifiers).", :cleannames!)
    oldnames = map(strip, names(df))
    newnames = map(n -> replace(n, r"\W", "_"), oldnames)
    names!(df, newnames)
    return
end

# rbind, cbind, vecbind

rbind(args...) = vcat(args...)

cbind(args...) = hcat(args...)

vecbind_type{T}(::Vector{T}) = Vector{T}
vecbind_type{T<:AbstractVector}(x::T) = Vector{eltype(x)}
vecbind_type{T<:AbstractDataVector}(x::T) = DataVector{eltype(x)}
vecbind_type{T}(::PooledDataVector{T}) = DataVector{T}
vecbind_type(v::StackedVector) = vecbind_promote_type(map(vecbind_type, v.components)...)
vecbind_type(v::RepeatedVector) = vecbind_type(v.parent)
vecbind_type(v::EachRepeatedVector) = vecbind_type(v.parent)

vecbind_promote_type{T1,T2}(x::Type{Vector{T1}}, y::Type{Vector{T2}}) = Array{promote_type(eltype(x), eltype(y)),1}
vecbind_promote_type{T1,T2}(x::Type{DataVector{T1}}, y::Type{DataVector{T2}}) = DataArray{promote_type(eltype(x), eltype(y)),1}
vecbind_promote_type{T1,T2}(x::Type{Vector{T1}}, y::Type{DataVector{T2}}) = DataArray{promote_type(eltype(x), eltype(y)),1}
vecbind_promote_type{T1,T2}(x::Type{DataVector{T1}}, y::Type{Vector{T2}}) = DataArray{promote_type(eltype(x), eltype(y)),1}
vecbind_promote_type(a, b, c, ds...) = vecbind_promote_type(a, vecbind_promote_type(b, c, ds...))
vecbind_promote_type(a, b, c) = vecbind_promote_type(a, vecbind_promote_type(b, c))

function vecbind_promote_type(a::AbstractVector)
    res = None
    if isdefined(a, 1)
        if length(a) == 1
            return a[1]
        else
            if isdefined(a, 2)
                res = vecbind_promote_type(a[1], a[2])
            else
                res = a[1]
            end
        end
    end
    for i in 3:length(a)
        if isdefined(a, i)
            res = vecbind_promote_type(res, a[i])
        end
    end
    return res
end

#constructor{T}(::Type{Vector{T}}, args...) = Array(T, args...)
#constructor{T}(::Type{DataVector{T}}, args...) = DataArray(T, args...)

function vecbind(xs::AbstractVector...)
    V = vecbind_promote_type(map(vecbind_type, {xs...}))
    len = sum(length, xs)
    res = constructor(V, len)
    k = 1
    for i in 1:length(xs)
        for j in 1:length(xs[i])
            res[k] = xs[i][j]
            k += 1
        end
    end
    res
end

function vecbind(xs::PooledDataVector...)
    vecbind(map(x -> convert(DataArray, x), xs)...)
end
