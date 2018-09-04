using Tables, IteratorInterfaceExtensions

Tables.istable(::Type{<:AbstractDataFrame}) = true
Tables.columnaccess(::Type{<:AbstractDataFrame}) = true
Tables.columns(df::AbstractDataFrame) = df
Tables.rowaccess(::Type{DataFrame}) = true
Tables.rows(df::DataFrame) = eachrow(df)

Tables.schema(df::AbstractDataFrame) = Tables.Schema(names(df), eltypes(df))

fromcolumns(x) = DataFrame(Any[collect(c) for c in Tables.eachcolumn(x)], collect(propertynames(x)))

function DataFrame(x::T) where {T}
    if Tables.istable(T)
        return fromcolumns(Tables.columns(x))
    end
    y = IteratorInterfaceExtensions.getiterator(x)
    yT = typeof(y)
    if Base.isiterable(yT)
        Base.depwarn("constructing a DataFrame from an iterator is deprecated; $T should support the Tables.jl interface", nothing)
        if Base.IteratorEltype(yT) === Base.HasEltype() && eltype(y) <: NamedTuple
            return fromcolumns(Tables.buildcolumns(Tables.Schema(eltype(y)), y))
        else
            # non-NamedTuple or UnknownEltype
            return fromcolumns(Tables.buildcolumns(nothing, y))
        end
    end
    throw(ArgumentError("unable to construct DataFrame from $T"))
end

Base.append!(df::DataFrame, x) = append!(df, DataFrame(x))

# This supports the Tables.RowTable type; needed to avoid ambiguities w/ another constructor
DataFrame(x::Vector{T}) where {T <: NamedTuple} = fromcolumns(Tables.columns(x))

IteratorInterfaceExtensions.getiterator(df::DataFrame) = Tables.datavaluerows(df)