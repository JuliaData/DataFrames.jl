# Super-hacked out constructor: DataVector[1, 2, NA]
function getindex(::Type{DataVector}, vals...)
    # Get the most generic non-NA type
    toptype = _dv_most_generic_type(vals)

    # Allocate an empty DataVector
    lenvals = length(vals)
    res = DataArray(Array(toptype, lenvals), BitArray(lenvals))

    # Copy from vals into data and mask
    for i = 1:lenvals
        if isna(vals[i])
            res.data[i] = baseval(toptype)
            res.na[i] = true
        else
            res.data[i] = vals[i]
            res.na[i] = false
        end
    end

    return res
end

##############################################################################
##
## isna()
##
##############################################################################

isna(x::Any) = false

##############################################################################
##
## find()
##
##############################################################################

function find(dv::AbstractDataVector{Bool})
    n = length(dv)
    res = Array(Int, n)
    bound = 0
    for i in 1:length(dv)
        if !isna(dv[i]) && dv[i]
            bound += 1
            res[bound] = i
        end
    end
    return res[1:bound]
end

##############################################################################
##
## String representations and printing
##
## TODO: Inherit these from AbstractArray after implementing DataArray
##
##############################################################################

head(dv::AbstractDataVector) = dv[1:min(6, length(dv))]
tail(dv::AbstractDataVector) = dv[max(length(dv) - 6, 1):length(dv)]

##############################################################################
##
## Container operations
##
##############################################################################

# TODO: Fill in definitions for PooledDataVector's
# TODO: Macroize these definitions

function push!{T}(dv::DataVector{T}, v::NAtype)
    push!(dv.data, baseval(T))
    push!(dv.na, true)
    return v
end

function push!{S, T}(dv::DataVector{S}, v::T)
    push!(dv.data, v)
    push!(dv.na, false)
    return v
end

function pop!(dv::DataVector)
    d, m = pop!(dv.data), pop!(dv.na)
    if m
        return NA
    else
        return d
    end
end

function unshift!{T}(dv::DataVector{T}, v::NAtype)
    unshift!(dv.data, baseval(T))
    unshift!(dv.na, true)
    return v
end

function unshift!{S, T}(dv::DataVector{S}, v::T)
    unshift!(dv.data, v)
    unshift!(dv.na, false)
    return v
end

function shift!{T}(dv::DataVector{T})
    d, m = shift!(dv.data), shift(dv.na)
    if m
        return NA
    else
        return d
    end
end

function map(f::Function, dv::DataVector)   # should this be an AbstractDataVector, so it works with PDV's?
    n = length(dv)
    res = DataArray(Any, n)
    for i in 1:n
        res[i] = f(dv[i])
    end
    return res
end

reverse(x::AbstractDataVector) = x[end:-1:1]

