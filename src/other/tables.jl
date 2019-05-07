using Tables, TableTraits, IteratorInterfaceExtensions

Tables.istable(::Type{<:AbstractDataFrame}) = true
Tables.columnaccess(::Type{<:AbstractDataFrame}) = true
Tables.columns(df::AbstractDataFrame) = df
Tables.rowaccess(::Type{<:AbstractDataFrame}) = true
Tables.rows(df::AbstractDataFrame) = Tables.rows(columntable(df))

Tables.schema(df::AbstractDataFrame) = Tables.Schema(names(df), eltypes(df))
Tables.materializer(df::AbstractDataFrame) = DataFrame

getvector(x::AbstractVector) = x
getvector(x) = collect(x)
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
    try
        return fromcolumns(Tables.columns(x), copycols=copycols)
    catch e
        throw(ArgumentError("unable to construct DataFrame from $(typeof(x))"))
    end
end

Base.append!(df::DataFrame, x) = append!(df, DataFrame(x, copycols=false))

# This supports the Tables.RowTable type; needed to avoid ambiguities w/ another constructor
function DataFrame(x::Vector{<:NamedTuple}; copycols::Bool=true)
    if !copycols
        throw(ArgumentError("It is not possible to construct a `DataFrame`" *
                            "from a `Vector{<:NamedTuple}` with `copycols=false`"))
    end
    fromcolumns(Tables.columns(Tables.IteratorWrapper(x)), copycols=false)
end
DataFrame!(x::Vector{<:NamedTuple}) =
    throw(ArgumentError("It is not possible to construct a `DataFrame` from " *
                        "`$(typeof(x))` without allocating new columns: use " *
                        "`DataFrame(x)` instead"))

IteratorInterfaceExtensions.getiterator(df::AbstractDataFrame) = Tables.datavaluerows(df)
IteratorInterfaceExtensions.isiterable(x::AbstractDataFrame) = true
TableTraits.isiterabletable(x::AbstractDataFrame) = true
