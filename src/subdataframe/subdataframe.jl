##############################################################################
##
## We use SubDataFrame's to maintain a reference to a subset of a DataFrame
## without making copies.
##
##############################################################################

# a SubDataFrame is a lightweight wrapper around a DataFrame used most
# frequently in split/apply sorts of operations.

immutable SubDataFrame{T <: AbstractVector{Int}} <: AbstractDataFrame
    parent::DataFrame
    rows::T # maps from subdf row indexes to parent row indexes

    function SubDataFrame(parent::DataFrame, rows::T)
        if length(rows) > 0
            rmin, rmax = extrema(rows)
            if rmin < 1 || rmax > size(parent, 1)
                throw(BoundsError())
            end
        end
        new(parent, rows)
    end
end

function SubDataFrame{T <: AbstractVector{Int}}(parent::DataFrame, rows::T)
    return SubDataFrame{T}(parent, rows)
end

function SubDataFrame(parent::DataFrame, row::Integer)
    return SubDataFrame(parent, [row])
end

function SubDataFrame{S <: Integer}(parent::DataFrame,
                                    rows::AbstractVector{S})
    return sub(parent, int(rows))
end

function Base.getindex(df::SubDataFrame, colinds::Any)
    return df.parent[df.rows, colinds]
end

function Base.getindex(df::SubDataFrame, rowinds::Any, colinds::Any)
    return df.parent[df.rows[rowinds], colinds]
end

function Base.setindex!(df::SubDataFrame, val::Any, colinds::Any)
    df.parent[df.rows, colinds] = val
    return df
end

function Base.setindex!(df::SubDataFrame, val::Any, rowinds::Any, colinds::Any)
    df.parent[df.rows[rowinds], colinds] = val
    return df
end

function Base.size(df::SubDataFrame)
    return length(df.rows), size(df.parent, 2)
end

function Base.size(df::SubDataFrame, i::Integer)
    if i == 1
        return length(df.rows)
    elseif i == 2
        return size(df.parent, 2)
    else
        throw(ArgumentError("Invalid size index"))
    end
end

Base.names(df::SubDataFrame) = names(df.parent)

# TODO: Remove this?
index(df::SubDataFrame) = index(df.parent)

function Base.sub{S <: Real}(df::DataFrame, rowinds::AbstractVector{S})
    return SubDataFrame(df, rowinds)
end

function Base.sub{S <: Real}(sdf::SubDataFrame, rowinds::AbstractVector{S})
    return SubDataFrame(sdf.parent, sdf.rows[rowinds])
end

function Base.sub(df::DataFrame, rowinds::AbstractVector{Bool})
    return sub(df, getindex(SimpleIndex(size(df, 1)), rowinds))
end

function Base.sub(sdf::SubDataFrame, rowinds::AbstractVector{Bool})
    return sub(sdf, getindex(SimpleIndex(size(sdf, 1)), rowinds))
end

function Base.sub(adf::AbstractDataFrame, rowinds::Integer)
    return SubDataFrame(adf, Int[rowinds])
end

function Base.sub(adf::AbstractDataFrame, rowinds::Any)
    return sub(adf, getindex(SimpleIndex(size(adf, 1)), rowinds))
end

function Base.sub(adf::AbstractDataFrame, rowinds::Any, colinds::Any)
    return sub(adf[[colinds]], rowinds)
end

function Base.delete!(df::SubDataFrame, c::Any)
    return SubDataFrame(delete!(df.parent, c), df.rows)
end

without(df::SubDataFrame, c::Any) = SubDataFrame(without(df.parent, c), df.rows)

Base.similar(df::SubDataFrame, dims) =
    DataFrame([similar(df[x], dims) for x in names(df)], names(df))

nas(df::SubDataFrame, dims) =
    DataFrame([nas(df[x], dims) for x in names(df)], names(df))
