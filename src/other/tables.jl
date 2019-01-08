using Tables, TableTraits, IteratorInterfaceExtensions

Tables.istable(::Type{<:AbstractDataFrame}) = true
Tables.columnaccess(::Type{<:AbstractDataFrame}) = true
Tables.columns(df::AbstractDataFrame) = df
Tables.rowaccess(::Type{DataFrame}) = true
Tables.rows(df::DataFrame) = Tables.rows(columntable(df))

Tables.schema(df::AbstractDataFrame) = Tables.Schema(names(df), eltypes(df))
Tables.materializer(df::DataFrame) = DataFrame

getvector(x::AbstractVector) = x
getvector(x) = collect(x)
fromcolumns(x) = DataFrame(Any[getvector(c) for c in Tables.eachcolumn(x)], Index(collect(Symbol, propertynames(x))))

function DataFrame(x)
    if Tables.istable(typeof(x))
        return fromcolumns(Tables.columns(x))
    end
    if TableTraits.supports_get_columns_copy_using_missing(x)
        y = TableTraits.get_columns_copy_using_missing(x)
        return DataFrame(collect(y), collect(keys(y)))
    end
    it = TableTraits.isiterabletable(x)
    if it === true
        # Base.depwarn("constructing a DataFrame from an iterator is deprecated; $T should support the Tables.jl interface", nothing)
        y = IteratorInterfaceExtensions.getiterator(x)
        return fromcolumns(Tables.columns(Tables.DataValueUnwrapper(y)))
    elseif it === missing
        if x isa AbstractVector && all(col -> isa(col, AbstractVector), x)
            return DataFrame(Vector{AbstractVector}(x))
        end
        y = IteratorInterfaceExtensions.getiterator(x)
        # non-NamedTuple or EltypeUnknown
        return fromcolumns(Tables.buildcolumns(nothing, Tables.DataValueUnwrapper(y)))
    end
    if x isa AbstractVector && all(col -> isa(col, AbstractVector), x)
        return DataFrame(Vector{AbstractVector}(x))
    end
    throw(ArgumentError("unable to construct DataFrame from $(typeof(x))"))
end

Base.append!(df::DataFrame, x) = append!(df, DataFrame(x))

# This supports the Tables.RowTable type; needed to avoid ambiguities w/ another constructor
DataFrame(x::Vector{T}) where {T <: NamedTuple} = fromcolumns(Tables.columns(Tables.DataValueUnwrapper(x)))

IteratorInterfaceExtensions.getiterator(df::AbstractDataFrame) = Tables.datavaluerows(df)
IteratorInterfaceExtensions.isiterable(x::AbstractDataFrame) = true
TableTraits.isiterabletable(x::AbstractDataFrame) = true
