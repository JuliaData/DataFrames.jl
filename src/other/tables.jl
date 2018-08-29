using Tables

Tables.schema(df::DataFrame) = NamedTuple{Tuple(names(df)), Tuple{eltypes(df)...}}
Tables.AccessStyle(df::DataFrame) = Tables.ColumnAccess()
Tables.rows(df::DataFrame) = eachrow(df)
Tables.columns(df::DataFrame) = df

function _isiterabletable(x::T) where {T}
    Base.isiterable(x) || return false
    return Base.IteratorEltype(x) === Base.HasEltype() ? eltype(x) <: NamedTuple : false
end

function DataFrame(x::T) where {T}
    if Tables.istable(T)
        DataFrame([collect(u) for u in Tables.columns(x)], collect(Tables.names(Tables.schema(x))))
    elseif _isiterabletable(x)
        _DataFrame(x)
    else
        convert(DataFrame, x)
    end
end

# needed to avoid ambiguities w/ another constructor; Tables.RowTable is just Vector{<:NamedTuple}
DataFrame(x::Tables.RowTable) = DataFrame([collect(u) for u in Tables.columns(x)],
                                          collect(Tables.names(Tables.schema(x))))