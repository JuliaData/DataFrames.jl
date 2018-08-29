using Tables

Tables.schema(df::DataFrame) = NamedTuple{Tuple(names(df)), Tuple{eltypes(df)...}}
Tables.AccessStyle(df::DataFrame) = Tables.ColumnAccess()
Tables.rows(df::DataFrame) = eachrow(df)
Tables.columns(df::DataFrame) = df

DataFrame(x::Tables.RowTable) = DataFrame([collect(u) for u in Tables.columns(x)],
                                          collect(Tables.names(Tables.schema(x))))
DataFrame(x::Any) = DataFrame([collect(u) for u in Tables.columns(x)],
                              collect(Tables.names(Tables.schema(x))))
