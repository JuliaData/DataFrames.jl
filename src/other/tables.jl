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
fromcolumns(x) = DataFrame(AbstractVector[getvector(c) for c in Tables.eachcolumn(x)], Index(collect(Symbol, propertynames(x))))

function DataFrame(x)
    if x isa AbstractVector && all(col -> isa(col, AbstractVector), x)
        return DataFrame(Vector{AbstractVector}(x))
    end
    if applicable(iterate, x)
        if all(v -> v isa Pair{Symbol, <:AbstractVector}, x)
            return DataFrame(AbstractVector[last(v) for v in x], [first(v) for v in x])
        end
    end
    if Tables.istable(x)
        return fromcolumns(Tables.columns(x))
    end
    throw(ArgumentError("unable to construct DataFrame from $(typeof(x))"))
end

Base.append!(df::DataFrame, x) = append!(df, DataFrame(x))

# This supports the Tables.RowTable type; needed to avoid ambiguities w/ another constructor
DataFrame(x::Vector{<:NamedTuple}) = fromcolumns(Tables.columns(Tables.IteratorWrapper(x)))

IteratorInterfaceExtensions.getiterator(df::AbstractDataFrame) = Tables.datavaluerows(df)
IteratorInterfaceExtensions.isiterable(x::AbstractDataFrame) = true
TableTraits.isiterabletable(x::AbstractDataFrame) = true
