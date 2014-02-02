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

