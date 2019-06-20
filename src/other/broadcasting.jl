### Broadcasting

Base.getindex(df::AbstractDataFrame, idx::CartesianIndex{2}) = df[idx[1], idx[2]]
Base.setindex!(df::AbstractDataFrame, val, idx::CartesianIndex{2}) =
    (df[idx[1], idx[2]] = val)

Base.broadcastable(df::AbstractDataFrame) = df

struct DataFrameStyle <: Base.Broadcast.BroadcastStyle end

Base.Broadcast.BroadcastStyle(::Type{<:AbstractDataFrame}) =
    DataFrameStyle()

Base.Broadcast.BroadcastStyle(::DataFrameStyle, ::Base.Broadcast.BroadcastStyle) = DataFrameStyle()
Base.Broadcast.BroadcastStyle(::Base.Broadcast.BroadcastStyle, ::DataFrameStyle) = DataFrameStyle()
Base.Broadcast.BroadcastStyle(::DataFrameStyle, ::DataFrameStyle) = DataFrameStyle()

function copyto_widen!(res::AbstractVector{T},
                       bc::Base.Broadcast.Broadcasted{DataFrameStyle},
                       pos, col) where T
    for i in pos:length(axes(bc)[1])
        val = bc[CartesianIndex(i, col)]
        S = typeof(val)
        if S <: T || promote_type(S, T) <: T
            res[i] = val
        else
            newres = similar(Vector{promote_type(S, T)}, length(res))
            copyto!(newres, 1, res, 1, i-1)
            newres[i] = val
            return copyto_widen!(newres, bc, i + 1, 2)
        end
    end
    return res
end

function getcolbc(bcf::Base.Broadcast.Broadcasted{Style}, colind) where {Style}
    # we assume that bcf is already flattened and unaliased
    Base.Broadcast.Broadcasted{Style}(bcf.f,
        map(x->Base.Broadcast.extrude(x isa AbstractDataFrame ? x[colind] : x),
            bcf.args), bcf.axes)
end

function Base.copy(bc::Base.Broadcast.Broadcasted{DataFrameStyle})
    bcf = Base.Broadcast.flatten(bc)
    colnames = unique([_names(df) for df in bcf.args if df isa AbstractDataFrame])
    if length(colnames) != 1
        wrongnames = setdiff(union(colnames...), intersect(colnames...))
        msg = join(wrongnames, ", ", " and ")
        throw(ArgumentError("Column names in broadcasted data frames must match. " *
                            "Non matching column names are $msg"))
    end
    nrows = length(axes(bcf)[1])
    df = DataFrame()
    for i in axes(bcf)[2]
        if nrows == 0
            col = Any[]
        else
            bcf′ = getcolbc(bcf, i)
            v1 = bcf′[CartesianIndex(1, i)]
            startcol = similar(Vector{typeof(v1)}, nrows)
            startcol[1] = v1
            col = copyto_widen!(startcol, bcf′, 2, i)
        end
        df[colnames[1][i]] = col
    end
    return df
end

### Broadcasting assignment

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
        if !haskey(index(df), idxs)
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

function Base.Broadcast.broadcast_unalias(dest::AbstractDataFrame, src)
    for col in eachcol(dest)
        col === src || (src = Base.Broadcast.unalias(col, src))
    end
    src
end

function Base.Broadcast.broadcast_unalias(dest::AbstractDataFrame, src::AbstractDataFrame)
    if size(dest) != size(src)
        throw(ArgumentError("Dimension mismatch in broadcasting."))
    end
    for col in axes(dest, 2)
        dcol = dest[col]
        scol = src[col]
        if dcol !== scol && Base.mightalias(dcol, scol)
            if src isa SubDataFrame
                src = SubDataFrame(copy(parent(src), copycols=false),
                                   index(src), rows(src))
                parentidx = parentcols(index(src), col)
                parent(src)[parentidx] = Base.unaliascopy(parent(src)[parentidx])
            else
                src = copy(src, copycols=false)
                src[col] = Base.unaliascopy(scol)
            end
        end
    end
    src
end

function Base.copyto!(df::AbstractDataFrame, bc::Base.Broadcast.Broadcasted)
    bcf = Base.Broadcast.flatten(bc)
    colnames = unique([_names(df) for df in bcf.args if df isa AbstractDataFrame])
    if length(colnames) > 1 || (length(colnames) == 1 && _names(df) != colnames[1])
        wrongnames = setdiff(union(colnames...), intersect(colnames...))
        msg = join(wrongnames, ", ", " and ")
        throw(ArgumentError("Column names in broadcasted data frames must match. " *
                            "Non matching column names are $msg"))
    end

    bcf′ = Base.Broadcast.preprocess(df, bcf)
    for i in axes(df, 2)
        _copyto_helper!(df[i], getcolbc(bcf′, i), i)
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

Base.Broadcast.broadcast_unalias(dest::DataFrameRow, src) =
    Base.Broadcast.broadcast_unalias(parent(dest), src)

function Base.copyto!(dfr::DataFrameRow, bc::Base.Broadcast.Broadcasted)
    bc′ = Base.Broadcast.preprocess(dfr, bc)
    for I in eachindex(bc′)
        dfr[I] = bc′[I]
    end
    dfr
end
