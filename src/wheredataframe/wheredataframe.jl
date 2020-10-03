"""
    WhereDataFrame{<:AbstractDataFrame,<:AbstractIndex}

    The result of a [`where`](@ref) operation on an `AbstractDataFrame`; a
    subset of a `AbstractDataFrame`
    Not meant to be constructed directly, see `where`.
"""
struct WhereDataFrame{D<:AbstractDataFrame, T<:AbstractVector{Int}}
    parent::D
    rows::T
end

Base.@propagate_inbounds function WhereDataFrame(parent::AbstractDataFrame, rows::AbstractVector{Bool})
    if length(rows) != nrow(parent)
        throw(ArgumentError("invalid length of `AbstractVector{Bool}` row index" *
                            " (got $(length(rows)), expected $(nrow(parent)))"))
    end
    return WhereDataFrame(parent, findall(rows))
end

rows(wdf::WhereDataFrame) = getfield(wdf, :rows)
Base.parent(wdf::WhereDataFrame) = getfield(wdf, :parent)


"""
    where(d::AbstractDataFrame, args...)

`where` introduces a `where` clause, which will be applied in the next function. Returns a `WhereDataFrame`.

`args...` obey the same syntax as `select(d, args...)`
Rows that return missing are understood as false

- `filter`/`filter!` returns an AbstractDataFrame after filtering (resp. deleting) specified rows
- `transform/transform!` returns an AbstractDataFrame with as many rows as the original `AbstractDataFrame` after applying the transformation on specified rows
- `combine` and `describe` return the same thing as the function applied to a view of the `AbstractDataFrame`

# Examples
```julia
julia> df = DataFrame(a = repeat([1, 2, 3, missing], outer=[2]),
               b = repeat([2, 1], outer=[4]),
               c = randn(8))

# filter rows that satisfies a certain condition
julia> filter(where(df, :a => ByRow(>(1))))
julia> filter(where(df, :a => ByRow(>(1)), :b => x -> x .< 2))

# transform only certain rows
julia> transform(where(df, :a => ByRow(ismissing)), :a => (x -> 0) => :a)
julia> transform(where(df, :a => ByRow(!ismissing)), :a => cumsum)

# combine using certain rows
julia> combine(where(df, :a => ByRow(!ismissing)), :a => sum)
```
"""
function where(df::AbstractDataFrame, args...)
    dfr = select(df, args...)
    if any(x -> !(eltype(x) <: Union{Bool, Missing}), eachcol(dfr))
        throw("Conditions do not evaluate to bool or missing")
    end
    if size(dfr, 2) == 1
        WhereDataFrame(df, coalesce.(dfr[!, 1], false))
    else
        WhereDataFrame(df, coalesce.(.&(eachcol(dfr)...), false))
    end
end

##############################################################################
##
## Show: show rows of parent that satisfies the `where` condition
## with original row number
##
##############################################################################

function Base.summary(io::IO, wdf::WhereDataFrame)
    print(io, "Where DataFrame")
end

function Base.show(io::IO,
          wdf::WhereDataFrame;
          allrows::Bool = !get(io, :limit, false),
          allcols::Bool = !get(io, :limit, false),
          splitcols = get(io, :limit, false),
          rowlabel::Symbol = :Row,
          eltypes::Bool = true,
          truncate::Int = 32)
    summary(io, wdf)
    _show(io, wdf, allrows=allrows, allcols=allcols, splitcols=splitcols,
          rowlabel=rowlabel, eltypes=eltypes, truncstring=truncate)
end

function _show(io::IO,
               wdf::WhereDataFrame;
               allrows::Bool = !get(io, :limit, false),
               allcols::Bool = !get(io, :limit, false),
               splitcols = get(io, :limit, false),
               rowlabel::Symbol = :Row,
               eltypes::Bool = true,
               truncstring::Int)

    df = parent(wdf)
    _check_consistency(df)

    # we will pass around this buffer to avoid its reallocation in ourstrwidth
    buffer = IOBuffer(Vector{UInt8}(undef, 80), read=true, write=true)

    nrows = length(rows(wdf))

    dsize = displaysize(io)
    availableheight = dsize[1] - 7
    nrowssubset = fld(availableheight, 2)
    bound = min(nrowssubset - 1, nrows)
    if allrows || nrows <= availableheight
        rowindices1 = rows(wdf)[1:nrows]
        rowindices2 = 1:0
    else
        rowindices1 = rows(wdf)[1:bound]
        rowindices2 = rows[max(bound + 1, nrows - nrowssubset + 1):nrows]
    end
    maxwidths = getmaxwidths(df, io, rowindices1, rowindices2, rowlabel, nothing,
                             eltypes, buffer, truncstring)
    width = getprintedwidth(maxwidths)
    showrows(io, df, rowindices1, rowindices2, maxwidths, splitcols, allcols,
             rowlabel, false, eltypes, nothing, buffer, truncstring)
    return
end


##############################################################################
##
## Common Operations
##
##############################################################################

Base.filter(wdf::WhereDataFrame) = parent(wdf)[rows(wdf), :]
Base.filter!(wdf::WhereDataFrame) = delete!(parent(wdf), setdiff(1:nrow(parent(wdf)), rows(wdf)))
Base.delete!(wdf::WhereDataFrame) = delete!(parent(wdf), rows(wdf))
Base.view(wdf::WhereDataFrame) = view(parent(wdf), rows(wdf), :)
#function nonunique(wdf::WhereDataFrame, args...)
#  x = falses(size(parent(wdf), 1))
#  idx = rows(wdf)[findall(nonunique(view(wdf), args...))]
#  x[idx] .= true
#  return x
#end
#Base.unique!(wdf::WhereDataFrame, args...) = delete!(parent(wdf), findall(nonunique(wdf, args...)))
#Base.unique(wdf::WhereDataFrame, args...) = parent(wdf)[(!).(nonunique(wdf, args...)), :]
combine(wdf::WhereDataFrame, args...; kwargs...) = combine(view(wdf), args...; kwargs...)

DataFrame(wdf::WhereDataFrame; copycols::Bool=true) = DataFrame(view(wdf); copycols = copycols)
Base.first(df::WhereDataFrame, args...) = first(view(df), args...)
Base.last(df::WhereDataFrame, args...) = last(view(df), args...)
DataAPI.describe(wdf::WhereDataFrame, args...; kwargs...) = describe(view(wdf), args...; kwargs...)

##############################################################################
##
## Select/transform
##
##############################################################################
#select!(wdf::WhereDataFrame, args...; renamecols::Bool=true) =
    #_replace_columns!(parent(wdf), select(wdf, args..., copycols=false, renamecols=renamecols))
#select(wdf::WhereDataFrame, args...; copycols::Bool=true, renamecols::Bool=true) =
    #manipulate(wdf, args..., copycols=copycols, renamecols=renamecols)
transform!(df::WhereDataFrame, args...; renamecols::Bool=true) =
    _replace_columns!(parent(wdf), transform(wdf, args..., copycols=false, renamecols=renamecols))
transform(wdf::WhereDataFrame, args...; copycols::Bool=true, renamecols::Bool=true) =
    manipulate(wdf, :, args..., copycols=copycols, renamecols=renamecols)

function manipulate(wdf::WhereDataFrame, cs...; copycols::Bool, renamecols::Bool)
    cs_vec = []
    for v in cs
        if v isa AbstractVector{<:Pair}
            append!(cs_vec, v)
        else
            push!(cs_vec, v)
        end
    end
    return _manipulate(wdf, [normalize_selection(index(parent(wdf)), c, renamecols) for c in cs_vec],
                    copycols)
end



function _manipulate(wdf::WhereDataFrame, normalized_cs, copycols::Bool)
    df = parent(wdf)
    newdf = DataFrame()
    transformed_cols = Dict{Symbol, Any}()
    for nc in normalized_cs
        if nc isa Pair
            newname = last(last(nc))
            @assert newname isa Symbol
            if haskey(transformed_cols, newname)
                throw(ArgumentError("duplicate target column name $newname passed"))
            end
            transformed_cols[newname] = nc
        end
    end
    for nc in normalized_cs
        if nc isa AbstractVector{Int}
            allunique(nc) || throw(ArgumentError("duplicate column names selected"))
            for i in nc
                newname = _names(df)[i]
                # as nc is a multiple column selection without transformations
                # we allow duplicate column names with selections applied earlier
                # and ignore them for convinience, to allow for e.g. select(df, :x1, :)
                if !hasproperty(newdf, newname)
                    if haskey(transformed_cols, newname)
                        # if newdf does not have a column newname
                        # but a column transformation was requested for this column
                        # then apply the transformation immediately
                        # in such a case nct may not be nothing, as if it were
                        # nothing then newname should be preasent in newdf already
                        nct = transformed_cols[newname]
                        @assert nct !== nothing
                        select_transform!(nct, wdf, newdf, transformed_cols, copycols)
                    else
                        # here even if keeprows is true all is OK
                        newdf[!, newname] = copycols ? df[:, i] : df[!, i]
                    end
                end
            end
        else
            # nc is normalized so it has a form src_cols => fun => Symbol
            newname = last(last(nc))
            if hasproperty(newdf, newname)
                # it is possible that the transformation has already been applied
                # via multiple column selection, like in select(df, :, :x1 => :y1)
                # but then transformed_cols[newname] must be nothing
                @assert transformed_cols[newname] === nothing
            else
                select_transform!(nc, wdf, newdf, transformed_cols, copycols)
            end
        end
    end
    return newdf
end



function select_transform!(nc::Pair{<:Union{Int, AbstractVector{Int}, AsTable},
                                    <:Pair{<:Base.Callable, Symbol}},
                           wdf::WhereDataFrame, newdf::DataFrame,
                           transformed_cols::Dict{Symbol, Any}, copycols::Bool)
    df = parent(wdf)
    col_idx, (fun, newname) = nc
    # It is allowed to request a tranformation operation into a newname column
    # only once. This is ensured by the logic related to transformed_cols dictionaly
    # in _manipulate, therefore in select_transform! such a duplicate should not happen
    @assert !hasproperty(newdf, newname)
    cdf = eachcol(df)
    if col_idx isa Int
        res = fun(view(df[!, col_idx], rows(wdf)))
    elseif col_idx isa AsTable
        res = fun(Tables.columntable(select(view(df, rows(wdf)), col_idx.cols, copycols=false)))
    else
        # it should be fast enough here as we do not expect to do it millions of times
        @assert col_idx isa AbstractVector{Int}
        res = fun(map(c -> view(cdf[c], rows(wdf)), col_idx)...)
    end
    if res isa Union{AbstractDataFrame, NamedTuple, DataFrameRow, AbstractMatrix}
        throw(ArgumentError("return value from function $fun " *
                            "of type $(typeof(res)) is currently not allowed."))
    end
    if res isa AbstractVector
        if ncol(newdf) == 0 && length(res) != length(rows(wdf))
            throw(ArgumentError("length $(length(res)) of vector returned from " *
                                "function $fun is different from number of rows " *
                                "$(nrow(df)) of the source data frame."))
        end
        respar = parent(res)
        parent_cols = col_idx isa AsTable ? col_idx.cols : col_idx
        if copycols && !(fun isa ByRow) &&
            (res isa SubArray || any(i -> respar === parent(cdf[i]), parent_cols))
            if newname ∈ propertynames(df)
                newdf[!, newname] = convert(Vector{Union{eltype(res), eltype(df[!, newname])}}, df[:, newname])
            else
                newdf[!, newname] = Vector{Union{eltype(res), Missing}}(missing, size(df, 1))
            end
            newdf[rows(wdf), newname] = copy(res)
        else
            if newname ∈ propertynames(df)
                newdf[!, newname] = convert(Vector{Union{eltype(res), eltype(df[!, newname])}}, df[:, newname])
            else
                newdf[!, newname] = Vector{Union{eltype(res), Missing}}(missing, size(df, 1))
            end
            newdf[rows(wdf), newname] = res
        end
    else
        res_unwrap = res isa Union{AbstractArray{<:Any, 0}, Ref} ? res[] : res
          if newname ∈ propertynames(df)
              newdf[!, newname] = convert(Vector{Union{eltype(res), eltype(df[!, newname])}}, df[:, newname])
          else
             newdf[!, newname] = Vector{Union{eltype(res), Missing}}(missing, size(df, 1))
        end
        newdf[rows(wdf), newname] = fill!(Tables.allocatecolumn(typeof(res_unwrap), length(rows(wdf))), res_unwrap)
    end
    # mark that column transformation was applied
    # nothing is not possible otherwise as a value in this dict
    transformed_cols[newname] = nothing
end


