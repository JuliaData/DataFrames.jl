struct LazyNewColDataFrame
    df::DataFrame
    col::Symbol
end

# we allow LazyNewColDataFrame only for data frames with at least one column
Base.axes(x::LazyNewColDataFrame) = axes(x.df[1])

Base.Broadcast.broadcastable(df::AbstractDataFrame) = Matrix(df)

Base.maybeview(df::AbstractDataFrame, idxs...) = view(df, idxs...)

function Base.maybeview(df::AbstractDataFrame, idxs)
    if ncol(df) == 0
        throw(ArgumentError("Broadcasting into a data frame with no columns is not allowed"))
    end
    if idxs isa Symbol
        if !haskey(df, idxs)
            if !(df isa DataFrame)
                throw(ArgumentError("column $idxs not found"))
            end
            return LazyNewColDataFrame(df, idxs)
        end
    end
    view(df, idxs)
end

function Base.copyto!(lazydf::LazyNewColDataFrame, bc)
    if isempty(lazydf.df)
        throw(ArgumentError("creating a column via broadcasting is not allowed on empty data frames"))
    end
    T = mapreduce(i -> typeof(bc[i]), promote_type, eachindex(bc); init=Union{})
    col = Tables.allocatecolumn(T, nrow(lazydf.df))
    copyto!(col, bc)
    lazydf.df[lazydf.col] = col
end

function Base.copyto!(df::AbstractDataFrame, bc)
    for I in eachindex(bc)
        # a safeguard against linear index
        row, col = Tuple(I)
        df[row, col] = bc[I]
    end
    df
end

function Base.copyto!(df::AbstractDataFrame, bc::Base.Broadcast.Broadcasted{<:Base.Broadcast.AbstractArrayStyle{0}})
    if bc.f === identity && bc.args isa Tuple{Any} && Base.Broadcast.isflat(bc)
        for col in axes(df, 2)
            fill!(df[col], bc.args[1][])
        end
        df
    else
        copyto!(df, convert(Broadcasted{Nothing}, bc))
    end
end

function Base.copyto!(dfr::DataFrameRow, bc)
    for I in eachindex(bc)
        dfr[I] = bc[I]
    end
    dfr
end
