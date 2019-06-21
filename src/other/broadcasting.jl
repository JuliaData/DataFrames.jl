struct LazyNewColDataFrame
    df::DataFrame
    col::Symbol
end

# we allow LazyNewColDataFrame only for data frames with at least one column
Base.axes(x::LazyNewColDataFrame) = (Base.OneTo(nrow(x.df)),)

Base.maybeview(df::AbstractDataFrame, idxs...) = view(df, idxs...)

function Base.maybeview(df::AbstractDataFrame, idxs)
    if ncol(df) == 0
        throw(ArgumentError("Broadcasting into a data frame with no columns is not allowed"))
    end
    if idxs isa Symbol
        if !hasproperty(df, idxs)
            if !(df isa DataFrame)
                # this will throw an appropriate error message
                df[idxs]
            end
            return LazyNewColDataFrame(df, idxs)
        end
    end
    view(df, idxs)
end

function Base.copyto!(lazydf::LazyNewColDataFrame, bc::Base.Broadcast.Broadcasted)
    if isempty(lazydf.df)
        throw(ArgumentError("creating a column via broadcasting is not allowed on empty data frames"))
    end
    if bc isa Base.Broadcast.Broadcasted{<:Base.Broadcast.AbstractArrayStyle{0}} &&
       bc.f === identity && bc.args isa Tuple{Any} && Base.Broadcast.isflat(bc)
        T = typeof(bc.args[1][])
        col = similar(Vector{T}, nrow(lazydf.df))
        copyto!(col, bc)
    else
        col = Base.Broadcast.materialize(bc)
    end
    lazydf.df[lazydf.col] = col
end

function _copyto_helper!(dfcol::AbstractVector, bc::Base.Broadcast.Broadcasted, col::Int)
    if axes(dfcol, 1) != axes(bc)[1]
        # this should never happen unless data frame is corrupted (has unequal column lengths)
        throw(ArgumentError("Dimension mismatch in broadcasting. " *
                            "The updated data frame is invalid and should not be used"))
    end
    @inbounds for row in eachindex(dfcol)
        dfcol[row] = bc[CartesianIndex(row, col)]
    end
end

function Base.copyto!(df::AbstractDataFrame, bc::Base.Broadcast.Broadcasted)
    for col in axes(df, 2)
        _copyto_helper!(df[col], bc, col)
    end
    df
end

function Base.copyto!(df::AbstractDataFrame, bc::Base.Broadcast.Broadcasted{<:Base.Broadcast.AbstractArrayStyle{0}})
    # special case of fast approach when bc is providing an untransformed scalar
    if bc.f === identity && bc.args isa Tuple{Any} && Base.Broadcast.isflat(bc)
        for col in axes(df, 2)
            fill!(df[col], bc.args[1][])
        end
        df
    else
        copyto!(df, convert(Broadcasted{Nothing}, bc))
    end
end

function Base.copyto!(dfr::DataFrameRow, bc::Base.Broadcast.Broadcasted)
    for I in eachindex(bc)
        dfr[I] = bc[I]
    end
    dfr
end
