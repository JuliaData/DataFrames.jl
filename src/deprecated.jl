import Base: @deprecate

@deprecate DataFrame(t::Type, nrows::Integer, ncols::Integer) DataFrame([Vector{t}(undef, nrows) for i in 1:ncols])

@deprecate DataFrame(column_eltypes::AbstractVector{<:Type},
                     nrows::Integer) DataFrame(column_eltypes, Symbol.('x' .* string.(1:length(column_eltypes))), nrows)

function DataFrame(column_eltypes::AbstractVector{T}, cnames::AbstractVector{Symbol},
                   categorical::AbstractVector{Bool}, nrows::Integer;
                   makeunique::Bool=false)::DataFrame where T<:Type
    Base.depwarn("`DataFrame` constructor with `categorical` positional argument is deprecated. " *
                 "Instead use `DataFrame(columns, names)` constructor.",
                 :DataFrame)
    updated_types = convert(Vector{Type}, column_eltypes)
    if length(categorical) != length(column_eltypes)
        throw(DimensionMismatch("arguments column_eltypes and categorical must have the same length " *
                                "(got $(length(column_eltypes)) and $(length(categorical)))"))
    end
    for i in eachindex(categorical)
        categorical[i] || continue
        elty = CategoricalArrays.catvaluetype(nonmissingtype(updated_types[i]),
                                              CategoricalArrays.DefaultRefType)
        if updated_types[i] >: Missing
            updated_types[i] = Union{elty, Missing}
        else
            updated_types[i] = elty
        end
    end
    return DataFrame(updated_types, cnames, nrows, makeunique=makeunique)
end

import Base: insert!
@deprecate insert!(df::DataFrame, df2::AbstractDataFrame) (foreach(col -> df[!, col] = df2[!, col], names(df2)); df)

import Base: show
@deprecate show(io::IO, df::AbstractDataFrame, allcols::Bool, rowlabel::Symbol, summary::Bool) show(io, df, allcols=allcols, rowlabel=rowlabel, summary=summary)
@deprecate show(io::IO, df::AbstractDataFrame, allcols::Bool, rowlabel::Symbol) show(io, df, allcols=allcols, rowlabel=rowlabel)
@deprecate show(io::IO, df::AbstractDataFrame, allcols::Bool) show(io, df, allcols=allcols)
@deprecate show(df::AbstractDataFrame, allcols::Bool, rowlabel::Symbol, summary::Bool) show(df, allcols=allcols, rowlabel=rowlabel, summary=summary)
@deprecate show(df::AbstractDataFrame, allcols::Bool, rowlabel::Symbol) show(df, allcols=allcols, rowlabel=rowlabel)
@deprecate show(df::AbstractDataFrame, allcols::Bool) show(df, allcols=allcols)

@deprecate showall(io::IO, df::AbstractDataFrame, allcols::Bool, rowlabel::Symbol, summary::Bool) show(io, df, allrows=true, allcols=allcols, rowlabel=rowlabel, summary=summary)
@deprecate showall(io::IO, df::AbstractDataFrame, allcols::Bool, rowlabel::Symbol) show(io, df, allrows=true, allcols=allcols, rowlabel=rowlabel)
@deprecate showall(io::IO, df::AbstractDataFrame, allcols::Bool = true) show(io, df, allrows=true, allcols=allcols)
@deprecate showall(df::AbstractDataFrame, allcols::Bool, rowlabel::Symbol, summary::Bool) show(df, allrows=true, allcols=allcols, rowlabel=rowlabel, summary=summary)
@deprecate showall(df::AbstractDataFrame, allcols::Bool, rowlabel::Symbol) show(df, allrows=true, allcols=allcols, rowlabel=rowlabel)
@deprecate showall(df::AbstractDataFrame, allcols::Bool = true) show(df, allrows=true, allcols=allcols)

@deprecate showall(io::IO, dfvec::AbstractVector{T}) where {T <: AbstractDataFrame} foreach(df->show(io, df, allrows=true, allcols=true), dfvec)
@deprecate showall(dfvec::AbstractVector{T}) where {T <: AbstractDataFrame} foreach(df->show(df, allrows=true, allcols=true), dfvec)

@deprecate showall(io::IO, df::GroupedDataFrame) show(io, df, allgroups=true)
@deprecate showall(df::GroupedDataFrame) show(df, allgroups=true)

import Base: delete!, insert!, merge!

@deprecate delete!(df::AbstractDataFrame, cols::Any) select!(df, Not(cols))
@deprecate insert!(df::DataFrame, col_ind::Int, item, name::Symbol; makeunique::Bool=false) insertcols!(df, col_ind, name => item; makeunique=makeunique)
@deprecate merge!(df1::DataFrame, df2::AbstractDataFrame) (foreach(col -> df1[!, col] = df2[!, col], names(df2)); df1)

import Base: map
@deprecate map(f::Function, sdf::SubDataFrame) f(sdf)
@deprecate map(f::Union{Function,Type}, dfc::DataFrameColumns{<:AbstractDataFrame, Pair{Symbol, AbstractVector}}) mapcols(f, dfc.df)

@deprecate head(df::AbstractDataFrame) first(df, 6)
@deprecate tail(df::AbstractDataFrame) last(df, 6)
@deprecate head(df::AbstractDataFrame, n::Integer) first(df, n)
@deprecate tail(df::AbstractDataFrame, n::Integer) last(df, n)

@deprecate SubDataFrame(df::AbstractDataFrame, rows::AbstractVector{<:Integer}) SubDataFrame(df, rows, :)
@deprecate SubDataFrame(df::AbstractDataFrame, ::Colon) SubDataFrame(df, :, :)

@deprecate colwise(f, d::AbstractDataFrame) [f(col) for col in eachcol(d)]
@deprecate colwise(fns::Union{AbstractVector, Tuple}, d::AbstractDataFrame) [f(col) for f in fns, col in eachcol(d)]
@deprecate colwise(f, gd::GroupedDataFrame) [[f(col) for col in eachcol(d)] for d in gd]
@deprecate colwise(fns::Union{AbstractVector, Tuple}, gd::GroupedDataFrame) [[f(col) for f in fns, col in eachcol(d)] for d in gd]

import Base: get
@deprecate get(df::AbstractDataFrame, key::Any, default::Any) key in names(df) ? df[!, key] : default

import Base: haskey
@deprecate haskey(df::AbstractDataFrame, key::Symbol) hasproperty(df, key)
@deprecate haskey(df::AbstractDataFrame, key::Integer) key in 1:ncol(df)
@deprecate haskey(df::AbstractDataFrame, key::Any) key in 1:ncol(df) || key in names(df)

import Base: empty!
@deprecate empty!(df::DataFrame) select!(df, Int[])

@deprecate deletecols!(df::DataFrame, inds) select!(df, Not(inds))
@deprecate deletecols(df::DataFrame, inds; copycols::Bool=true) select(df, Not(inds), copycols=copycols)

import Base: getindex
@deprecate getindex(df::DataFrame, col_ind::ColumnIndex) df[!, col_ind]
@deprecate getindex(df::DataFrame, col_inds::Union{AbstractVector, Regex, Not}) df[:, col_inds]
@deprecate getindex(df::DataFrame, ::Colon) df[:, :]
@deprecate getindex(sdf::SubDataFrame, colind::ColumnIndex) sdf[!, colind]
@deprecate getindex(sdf::SubDataFrame, colinds::Union{AbstractVector, Regex, Not}) sdf[!, colinds]
@deprecate getindex(sdf::SubDataFrame, ::Colon) sdf[!, :]

import Base: view
@deprecate view(adf::AbstractDataFrame, colind::ColumnIndex) view(adf, :, colind)
@deprecate view(adf::AbstractDataFrame, colinds) view(adf, :, colinds)

import Base: setindex!
@deprecate setindex!(df::DataFrame, x::Nothing, col_ind::Int) select!(df, Not(col_ind))
@deprecate setindex!(sdf::SubDataFrame, val::Any, colinds::Any) (sdf[:, colinds] = val; sdf)
@deprecate setindex!(df::DataFrame, v::AbstractVector, col_ind::ColumnIndex) (df[!, col_ind] = v; df)

# df[SingleColumnIndex] = Single Item (EXPANDS TO NROW(df) if NCOL(df) > 0)
function Base.setindex!(df::DataFrame, v, col_ind::ColumnIndex)
    if haskey(index(df), col_ind)
        df[:, col_ind] .= v
        Base.depwarn("Implicit broadcasting to an existing column in DataFrame assignment is deprecated." *
                     "Use an explicit broadcast with `df[:, col_ind] .= v`", :setindex!)
    else
        if ncol(df) == 0
            df[!, col_ind] = [v]
            Base.depwarn("Implicit broadcasting to a new column in DataFrame assignment is deprecated." *
                         "Use `df[!, col_ind] = [v]` when `df` has zero columns", :setindex!)
        else
            df[!, col_ind] .= v
            Base.depwarn("Implicit broadcasting to a new column in DataFrame assignment is deprecated." *
                         "Use `df[!, col_ind] .= v`  when `df` has some columns", :setindex!)
        end
    end
    return df
end

# df[MultiColumnIndex] = DataFrame
function Base.setindex!(df::DataFrame, new_df::DataFrame, col_inds::AbstractVector{Bool})
    setindex!(df, new_df, findall(col_inds))
end
@deprecate setindex!(df::DataFrame, new_df::DataFrame,
                     col_inds::AbstractVector{<:ColumnIndex}) foreach(((j, colind),) -> (df[!, colind] = new_df[!, j]),
                                                                      enumerate(col_inds))

# df[MultiColumnIndex] = AbstractVector (REPEATED FOR EACH COLUMN)
@deprecate setindex!(df::DataFrame, v::AbstractVector,
                     col_inds::AbstractVector{Bool}) (foreach(c -> (df[!, c] = copy(v)), findall(col_inds)); df)
@deprecate setindex!(df::DataFrame, v::AbstractVector,
                     col_inds::AbstractVector{<:ColumnIndex}) (foreach(c -> (df[!, c] = copy(v)), col_inds); df)

# df[MultiColumnIndex] = Single Item (REPEATED FOR EACH COLUMN; EXPANDS TO NROW(df) if NCOL(df) > 0)
function Base.setindex!(df::DataFrame,
                        val::Any,
                        col_inds::AbstractVector{Bool})
    setindex!(df, val, findall(col_inds))
end
function Base.setindex!(df::DataFrame, val::Any, col_inds::AbstractVector{<:ColumnIndex})
    for col_ind in col_inds
        df[col_ind] = val
    end
    Base.depwarn("implicit broadcasting in setindex! is deprecated; " *
                 "use `df[:, col_inds] .= Ref(v)` broadcasting assignment to change the columns in place", :setindex!)
    return df
end

# df[:] = AbstractVector or Single Item
function Base.setindex!(df::DataFrame, v, ::Colon)
    df[1:size(df, 2)] = v
    Base.depwarn("`df[:] = v` syntax is deprecated; " *
                 "use `df[:, :] .= Ref(v)` broadcasting assignment to change the columns in place", :setindex!)
    df
end

# df[SingleRowIndex, MultiColumnIndex] = 1-Row DataFrame
@deprecate setindex!(df::DataFrame, new_df::DataFrame, row_ind::Integer,
                     col_inds::AbstractVector{Bool}) (foreach(((i, c),) -> (df[row_ind, c] = new_df[1, i]),
                                                              enumerate(findall(col_inds))); df)
@deprecate setindex!(df::DataFrame, new_df::DataFrame, row_ind::Integer,
                     col_inds::AbstractVector{<:ColumnIndex}) (foreach(((i, c),) -> (df[row_ind, c] = new_df[1, i]),
                                                                       enumerate(col_inds)); df)

# df[MultiRowIndex, SingleColumnIndex] = Single Item
function Base.setindex!(df::DataFrame,
                        v::Any,
                        row_inds::AbstractVector,
                        col_ind::ColumnIndex)
    insert_multiple_entries!(df, v, row_inds, col_ind)
    Base.depwarn("implicit broadcasting in setindex! is deprecated; " *
                 "use `df[row_inds, col_ind] .= Ref(v)` broadcasting assignment to change the column in place", :setindex!)
    return df
end

# df[MultiRowIndex, MultiColumnIndex] = AbstractVector
function Base.setindex!(df::DataFrame,
                        v::AbstractVector,
                        row_inds::AbstractVector,
                        col_inds::AbstractVector)
    col_inds_norm = index(df)[col_inds]
    for col_ind in col_inds_norm
        insert_multiple_entries!(df, v, row_inds, col_ind)
    end
    Base.depwarn("implicit vector broadcasting in setindex! is deprecated; " *
                 "use `df[row_inds, col_inds] .= v` broadcasting assignment to change the columns in place", :setindex!)
    return df
end

# df[MultiRowIndex, MultiColumnIndex] = Single Item
function Base.setindex!(df::DataFrame,
                        v::Any,
                        row_inds::AbstractVector,
                        col_inds::AbstractVector)
    col_inds_norm = index(df)[col_inds]
    for col_ind in col_inds_norm
        insert_multiple_entries!(df, v, row_inds, col_ind)
    end
    Base.depwarn("implicit vector broadcasting in setindex! is deprecated; " *
                 "use `df[row_inds, col_inds] .= Ref(v)` broadcasting assignment to change the columns in place", :setindex!)
    return df
end

# df[:, :] = ...
function Base.setindex!(df::DataFrame, v, ::Colon, ::Colon)
    df[1:size(df, 1), 1:size(df, 2)] = v
    Base.depwarn("implicit vector broadcasting in setindex! is deprecated; " *
        "use `df[:, col_inds] .= Ref(v)` broadcasting assignment to change the columns in place", :setindex!)
    df
end

# df[Any, :] = ...
function Base.setindex!(df::DataFrame, v, row_inds, ::Colon)
    df[row_inds, 1:size(df, 2)] = v
    Base.depwarn("implicit vector broadcasting in setindex! is deprecated; " *
                 "use `df[row_inds, col_inds] .= Ref(v)` broadcasting assignment", :setindex!)
    df
end

# df[:, Any] = ...
function Base.setindex!(df::DataFrame, v, ::Colon, col_inds)
    df[col_inds] = v
    Base.depwarn("implicit vector broadcasting in setindex! is deprecated; " *
                 "use `df[:, col_inds] .= Ref(v)` broadcasting assignment to change the columns in place", :setindex!)
    df
end

function Base.dotview(df::AbstractDataFrame, col::ColumnIndex)
    Base.depwarn("in broadcasted assignment use `df[:, col]` instead of `df[col]`", :dotview)
    @view df[:, col]
end

import Base: setproperty!
@deprecate setproperty!(df::DataFrame, col_ind::Symbol, v) (df[!, col_ind] .= v)
@deprecate setproperty!(df::SubDataFrame, col_ind::Symbol, v) (df[:, col_ind] .= v)

@deprecate eltypes(df::AbstractDataFrame) eltype.(eachcol(df))

@deprecate permutecols!(df::DataFrame, p::AbstractVector) select!(df, p)
@deprecate names!(x::Index, nms::Vector{Symbol};
                  makeunique::Bool=false) rename!(x, nms, makeunique=makeunique)
@deprecate names!(df::AbstractDataFrame, vals::Vector{Symbol};
                  makeunique::Bool=false) rename!(df, vals, makeunique=makeunique)

import DataAPI: describe
@deprecate describe(io::IO, df::AbstractDataFrame, stats::Union{Symbol, Pair{Symbol}}...;
                    cols=:) describe(df, stats..., cols=cols)

@deprecate stackdf(args...; kwargs...) stack(args...; kwargs..., view=true)
@deprecate meltdf(args...; kwargs...) melt(args...; kwargs..., view=true)

@deprecate melt(df::AbstractDataFrame, id_vars;
                variable_name::Symbol=:variable, value_name::Symbol=:value,
                view::Bool=false) stack(df, Not(id_vars); variable_name=variable_name,
                                        value_name=value_name, view=view)

@deprecate melt(df::AbstractDataFrame, id_vars, measure_vars;
                variable_name::Symbol=:variable, value_name::Symbol=:value,
                view::Bool=false) stack(df, measure_vars, id_vars; variable_name=variable_name,
                                        value_name=value_name, view=view)
@deprecate melt(df::AbstractDataFrame; variable_name::Symbol=:variable, value_name::Symbol=:value,
                view::Bool=false) stack(df; variable_name=variable_name, value_name=value_name,
                                        view=view)

import Base: lastindex
@deprecate lastindex(df::AbstractDataFrame) ncol(df)
