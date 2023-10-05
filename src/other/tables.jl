Tables.istable(::Type{<:AbstractDataFrame}) = true
Tables.columnaccess(::Type{<:AbstractDataFrame}) = true
Tables.columns(df::AbstractDataFrame) = eachcol(df)
Tables.rowaccess(::Type{<:AbstractDataFrame}) = true
Tables.rows(df::AbstractDataFrame) = eachrow(df)
Tables.rowtable(df::AbstractDataFrame) = Tables.rowtable(Tables.columntable(df))
Tables.namedtupleiterator(df::AbstractDataFrame) =
    Tables.namedtupleiterator(Tables.columntable(df))

function Tables.columnindex(df::Union{AbstractDataFrame, DataFrameRow}, idx::Symbol)
    ind = index(df)
    if ind isa Index
        return get(ind.lookup, idx, 0)
    else
        parent_ind = ind.parent
        loc = get(parent_ind.lookup, idx, 0)
        return loc == 0 || loc > length(ind.remap) ? 0 : max(0, ind.remap[loc])
    end
end

Tables.columnindex(df::Union{AbstractDataFrame, DataFrameRow}, idx::AbstractString) =
    columnindex(df, Symbol(idx))

Tables.schema(df::AbstractDataFrame) = Tables.Schema(_names(df), [eltype(col) for col in eachcol(df)])
Tables.materializer(::Type{<:AbstractDataFrame}) = DataFrame

Tables.getcolumn(df::AbstractDataFrame, i::Int) = df[!, i]
Tables.getcolumn(df::AbstractDataFrame, nm::Symbol) = df[!, nm]

Tables.getcolumn(dfr::DataFrameRow, i::Int) = dfr[i]
Tables.getcolumn(dfr::DataFrameRow, nm::Symbol) = dfr[nm]

getvector(x::AbstractVector) = x
getvector(x) = [x[i] for i = 1:length(x)]

fromcolumns(x, names; copycols::Union{Nothing, Bool}=nothing) =
    DataFrame(AbstractVector[getvector(Tables.getcolumn(x, nm)) for nm in names],
              Index(names),
              copycols=something(copycols, true))

# note that copycols is false by default in this definition (Tables.CopiedColumns
# implies copies have already been made) but if `copycols=true`, a copy will still be
# made; this is useful for scenarios where the input is immutable so avoiding copies
# is desirable, but you may still want a copy for mutation (Arrow.Table is like this)
fromcolumns(x::Tables.CopiedColumns, names; copycols::Union{Nothing, Bool}=nothing) =
    fromcolumns(Tables.source(x), names; copycols=something(copycols, false))

function DataFrame(x; copycols::Union{Nothing, Bool}=nothing)
    # here we handle eltypes not specific enough to be dispatched
    # to other DataFrames constructors taking vector of `Pair`s
    if !Tables.istable(x) && x isa AbstractVector && !isempty(x) &&
        (all(v -> v isa Pair{Symbol, <:AbstractVector}, x) ||
         all(v -> v isa Pair{<:AbstractString, <:AbstractVector}, x))
        df = DataFrame(AbstractVector[last(v) for v in x], [first(v) for v in x],
                       copycols=something(copycols, true))
    else
        cols = Tables.columns(x)
        names = collect(Symbol, Tables.columnnames(cols))
        df = fromcolumns(cols, names, copycols=copycols)
    end
    _copy_all_all_metadata!(df, x)
    return df
end

# the logic here relies on the fact that Tables.CopiedColumns
# is the only exception for default copycols value
DataFrame(x, cnames::AbstractVector; makeunique::Bool=false,
          copycols::Union{Nothing, Bool}=nothing) =
    rename!(DataFrame(x, copycols=something(copycols, !(x isa Tables.CopiedColumns))),
            _name2symbol(cnames),
            makeunique=makeunique)

# This supports the Tables.RowTable type; needed to avoid ambiguities w/ another constructor
DataFrame(x::AbstractVector{NamedTuple{names, T}}; copycols::Bool=true) where {names, T} =
    fromcolumns(Tables.columns(Tables.IteratorWrapper(x)), collect(names), copycols=false)

Tables.istable(::Type{<:Union{DataFrameRows, DataFrameColumns}}) = true
Tables.columnaccess(::Type{<:Union{DataFrameRows, DataFrameColumns}}) = true
Tables.rowaccess(::Type{<:Union{DataFrameRows, DataFrameColumns}}) = true
Tables.columns(itr::Union{DataFrameRows, DataFrameColumns}) = Tables.columns(parent(itr))
Tables.rows(itr::Union{DataFrameRows, DataFrameColumns}) = Tables.rows(parent(itr))
Tables.schema(itr::Union{DataFrameRows, DataFrameColumns}) = Tables.schema(parent(itr))
Tables.rowtable(itr::Union{DataFrameRows, DataFrameColumns}) = Tables.rowtable(parent(itr))
Tables.namedtupleiterator(itr::Union{DataFrameRows, DataFrameColumns}) =
    Tables.namedtupleiterator(parent(itr))
Tables.materializer(::Type{<:Union{DataFrameRows, DataFrameColumns}}) =
    DataFrame

Tables.getcolumn(itr::Union{DataFrameRows, DataFrameColumns}, i::Int) =
    Tables.getcolumn(parent(itr), i)
Tables.getcolumn(itr::Union{DataFrameRows, DataFrameColumns}, nm::Symbol) =
    Tables.getcolumn(parent(itr), nm)

IteratorInterfaceExtensions.getiterator(df::AbstractDataFrame) =
    Tables.datavaluerows(Tables.columntable(df))
IteratorInterfaceExtensions.isiterable(x::AbstractDataFrame) = true
TableTraits.isiterabletable(x::AbstractDataFrame) = true

@inline function Tables.subset(df::AbstractDataFrame, inds; viewhint::Union{Bool, Nothing}=nothing)
    res = viewhint === true ? view(df, inds, :) : df[inds, :]
    if res isa DataFrameRow && viewhint === false
        return copy(res)
    else
        return res
    end
end

@inline function Tables.subset(itr::DataFrameRows, inds; viewhint::Union{Bool, Nothing}=nothing)
    res = Tables.subset(parent(itr), inds, viewhint=viewhint)
    return res isa AbstractDataFrame ? eachrow(res) : res
end

@inline function Tables.subset(itr::DataFrameColumns, inds; viewhint::Union{Bool, Nothing}=nothing)
    res = Tables.subset(parent(itr), inds, viewhint=viewhint)
    return res isa AbstractDataFrame ? eachcol(res) : res
end
