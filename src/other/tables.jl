Tables.istable(::Type{<:AbstractDataFrame}) = true
Tables.columnaccess(::Type{<:AbstractDataFrame}) = true
Tables.columns(df::AbstractDataFrame) = df
Tables.rowaccess(::Type{<:AbstractDataFrame}) = true
Tables.rows(df::AbstractDataFrame) = Tables.rows(columntable(df))

Tables.schema(df::AbstractDataFrame) = Tables.Schema(names(df), eltype.(eachcol(df)))
Tables.materializer(df::AbstractDataFrame) = DataFrame

getvector(x::AbstractVector) = x
getvector(x) = collect(x)
# note that copycols is ignored in this definition (Tables.CopiedColumns implies copies have already been made)
fromcolumns(x::Tables.CopiedColumns; copycols::Bool=true) =
    DataFrame(AbstractVector[getvector(c) for c in Tables.eachcolumn(x)],
              Index(collect(Symbol, propertynames(x))),
              copycols=false)
fromcolumns(x; copycols::Bool=true) =
    DataFrame(AbstractVector[getvector(c) for c in Tables.eachcolumn(x)],
              Index(collect(Symbol, propertynames(x))),
              copycols=copycols)

function DataFrame(x::T; copycols::Bool=true) where {T}
    if x isa AbstractVector && all(col -> isa(col, AbstractVector), x)
        return DataFrame(Vector{AbstractVector}(x), copycols=copycols)
    end
    if x isa AbstractVector || x isa Tuple
        if all(v -> v isa Pair{Symbol, <:AbstractVector}, x)
            return DataFrame(AbstractVector[last(v) for v in x], [first(v) for v in x],
                             copycols=copycols)
        end
    end
    return fromcolumns(Tables.columns(x), copycols=copycols)
end

function Base.append!(df::DataFrame, table; cols::Symbol=:setequal)
    if table isa Dict && cols == :orderequal
        throw(ArgumentError("passing `Dict` as `table` when `cols` is equal to " *
                            "`:orderequal` is not allowed as it is unordered"))
    end
    append!(df, DataFrame(table, copycols=false), cols=cols)
end

# This supports the Tables.RowTable type; needed to avoid ambiguities w/ another constructor
DataFrame(x::Vector{<:NamedTuple}; copycols::Bool=true) =
    fromcolumns(Tables.columns(Tables.IteratorWrapper(x)), copycols=false)
DataFrame!(x::Vector{<:NamedTuple}) =
    throw(ArgumentError("It is not possible to construct a `DataFrame` from " *
                        "`$(typeof(x))` without allocating new columns: use " *
                        "`DataFrame(x)` instead"))

for T in [DataFrameRows, DataFrameColumns]
    @eval begin
        Tables.istable(::Type{<:$T}) = true
        Tables.columnaccess(::Type{<:$T}) = true
        Tables.rowaccess(::Type{<:$T}) = true
        Tables.columns(itr::$T) = Tables.columns(getfield(itr, :df))
        Tables.rows(itr::$T) = Tables.rows(getfield(itr, :df))
        Tables.schema(itr::$T) = Tables.schema(getfield(itr, :df))
        Tables.materializer(itr::$T) = Tables.materializer(getfield(itr, :df))
    end
end

IteratorInterfaceExtensions.getiterator(df::AbstractDataFrame) = Tables.datavaluerows(df)
IteratorInterfaceExtensions.isiterable(x::AbstractDataFrame) = true
TableTraits.isiterabletable(x::AbstractDataFrame) = true
