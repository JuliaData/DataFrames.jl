using Tables, IteratorInterfaceExtensions

Tables.istable(::Type{<:AbstractDataFrame}) = true
Tables.columnaccess(::Type{<:AbstractDataFrame}) = true
Tables.columns(df::AbstractDataFrame) = df
Tables.rowaccess(::Type{DataFrame}) = true
Tables.rows(df::DataFrame) = eachrow(df)

Tables.schema(df::AbstractDataFrame) = Tables.Schema(names(df), eltypes(df))

function _isiterabletable(x::T) where {T}
    Base.isiterable(T) || return false
    return Base.IteratorEltype(x) === Base.HasEltype() ? eltype(x) <: NamedTuple : false
end

function DataFrame(x::T) where {T}
    if Tables.istable(T)
        columns = Tables.columns(x)
        return DataFrame([collect(u) for u in Tables.eachcolumn(columns)],
                          collect(Tables.schema(columns).names))
    end
    y = IteratorInterfaceExtensions.getiterator(x)
    yT = typeof(y)
    if Base.isiterable(yT)
        if Base.IteratorEltype(yT) === Base.HasEltype() && eltype(y) <: NamedTuple
            columns = Tables.buildcolumns(Tables.Schema(eltype(y)), y)
            return DataFrame([collect(u) for u in columns],
                        collect(Tables.schema(columns).names))
        else
            # non-NamedTuple or UnknownEltype
            columns = Tables.buildcolumns(Tables.Schema(propertynames(first(x)), nothing), y)
            return DataFrame([collect(u) for u in columns],
                        collect(Tables.schema(columns).names))
        end
    end
    throw(ArgumentError("unable to construct DataFrame from $T"))
end

Base.append!(df::DataFrame, x) = append!(df, DataFrame(x))

# This supports the Tables.RowTable type; needed to avoid ambiguities w/ another constructor
function DataFrame(x::Vector{T}) where {T <: NamedTuple}
    columns = Tables.columns(x)
    DataFrame([collect(u) for u in Tables.eachcolumn(columns)],
               collect(Tables.schema(columns).names))
end

IteratorInterfaceExtensions.getiterator(df::DataFrame) = Tables.datavaluerows(df)