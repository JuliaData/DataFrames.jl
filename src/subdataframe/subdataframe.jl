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

function SubDataFrame{S <: Integer}(parent::DataFrame, rows::AbstractVector{S})
    return sub(parent, int(rows))
end


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

##############################################################################
##
## AbstractDataFrame interface
##
##############################################################################

index(sdf::SubDataFrame) = index(sdf.parent)

# TODO: Remove these
nrow(sdf::SubDataFrame) = ncol(sdf) > 0 ? length(sdf.rows)::Int : 0
ncol(sdf::SubDataFrame) = length(index(sdf))

function Base.getindex(sdf::SubDataFrame, colinds::Any)
    return sdf.parent[sdf.rows, colinds]
end

function Base.getindex(sdf::SubDataFrame, rowinds::Any, colinds::Any)
    return sdf.parent[sdf.rows[rowinds], colinds]
end

function Base.setindex!(sdf::SubDataFrame, val::Any, colinds::Any)
    sdf.parent[sdf.rows, colinds] = val
    return sdf
end

function Base.setindex!(sdf::SubDataFrame, val::Any, rowinds::Any, colinds::Any)
    sdf.parent[sdf.rows[rowinds], colinds] = val
    return sdf
end

##############################################################################
##
## Miscellaneous
##
##############################################################################

Base.map(f::Function, sdf::SubDataFrame) = f(sdf) # TODO: deprecate

function Base.delete!(sdf::SubDataFrame, c::Any) # TODO: deprecate?
    return SubDataFrame(delete!(sdf.parent, c), sdf.rows)
end

without(sdf::SubDataFrame, c::Vector{Int}) = sub(without(sdf.parent, c), sdf.rows)
without(sdf::SubDataFrame, c::Int) = sub(without(sdf.parent, c), sdf.rows)
without(sdf::SubDataFrame, c::Any) = sub(without(sdf.parent, c), sdf.rows)
