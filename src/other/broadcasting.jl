struct DummyColumnValue end

Base.Broadcast.broadcastable(df::AbstractDataFrame) = Matrix(df)

Base.maybeview(df::AbstractDataFrame, idxs...) = view(df, idxs...)

function Base.maybeview(df::AbstractDataFrame, idxs)
    if idxs isa Symbol
        if !haskey(df, idxs)
            if !(df isa DataFrame)
                throw(ArgumentError("column $idxs not found"))
            end
            df[idxs] = Vector{DummyColumnValue}(undef, nrow(df))
            return view(df, [idxs])
        end
    end
    if idxs isa AbstractVector{Symbol}
        for idx in idxs
            if !haskey(df, idx)
                if !(df isa DataFrame)
                    throw(ArgumentError("column $idxs not found"))
                end
                df[idx] = Vector{DummyColumnValue}(undef, nrow(df))
            end
        end
    end
    view(df, idxs)
end

function Base.copyto!(df::AbstractDataFrame, bc)
    m = Base.materialize(bc)
    if ncol(df) == 1 && eltype(df[1]) === DummyColumnValue
        if maximum(ndims.(Base.Broadcast.flatten(bc).args)) > 1
            throw(DimensionMismatch("cannot broadcast array to have fewer dimensions"))
        end
    end
    for col in axes(df, 2)
        if eltype(df[col]) === DummyColumnValue
            colname = names(df)[col]
            T = reduce(promote_type, typeof.(view(m, :, col)))
            parent(df)[colname] = Tables.allocatecolumn(T, nrow(df))
        end
        df[col] .= view(m, :, col)
    end
    df
end

function Base.copyto!(df::AbstractDataFrame, bc::Base.Broadcast.Broadcasted{<:Base.Broadcast.AbstractArrayStyle{0}})
    if bc.f === identity && bc.args isa Tuple{Any} && Base.Broadcast.isflat(bc)
        for col in axes(df, 2)
            if eltype(df[col]) === DummyColumnValue
                colname = names(df)[col]
                parent(df)[colname] = Tables.allocatecolumn(typeof(bc.args[1][]), nrow(df))
            end
            fill!(df[col], bc.args[1][])
        end
        df
    else
        copyto!(df, convert(Broadcasted{Nothing}, bc))
    end
end

function Base.copyto!(dfr::DataFrameRow, x)
    m = Base.materialize(x)
    foreach(col -> dfr[col] = m[col], 1:length(dfr))
    dfr
end

Base.copyto!(dfr::DataFrameRow, bc::Base.Broadcast.Broadcasted{<:Base.Broadcast.AbstractArrayStyle{0}}) =
    if bc.f === identity && bc.args isa Tuple{Any} && Base.Broadcast.isflat(bc)
        foreach(col -> dfr[col] = bc.args[1][], 1:length(dfr))
        dfr
    else
        copyto!(dfr, convert(Broadcasted{Nothing}, bc))
    end

function Base.Broadcast.materialize!(dest::SubDataFrame, bc::Base.Broadcast.Broadcasted{Style}) where {Style}
    try
        copyto!(dest, Base.Broadcast.instantiate(Base.Broadcast.Broadcasted{Style}(bc.f, bc.args, axes(dest))))
    catch
        df = parent(dest)
        deletecols!(df, findall(col -> eltype(col) == DummyColumnValue, eachcol(df)))
        rethrow()
    end
end
function Base.Broadcast.materialize!(dest::SubDataFrame, x)
    try
        copyto!(dest, Base.Broadcast.instantiate(Base.Broadcast.Broadcasted(identity, (x,), axes(dest))))
    catch
        df = parent(dest)
        deletecols!(df, findall(col -> eltype(col) == DummyColumnValue, eachcol(df)))
        rethrow()
    end
end
