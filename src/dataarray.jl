##############################################################################
##
## AbstractDataArray is a type of AbstractArray that can handle NA's
##
##############################################################################

abstract AbstractDataArray{T, N} <: AbstractArray{T, N}

##############################################################################
##
## DataArray type definition
##
##############################################################################

type DataArray{T, N} <: AbstractDataArray{T, N}
    data::Array{T, N}
    na::BitArray{N}

    # Sanity check that new data values and missingness metadata match
    function DataArray(d::Array{T, N}, m::BitArray{N})
        if size(d) != size(m)
            msg = "Data and missingness arrays must be the same size"
            throw(ArgumentError(msg))
        end
        new(d, m)
    end
end

##############################################################################
##
## DataVector and DataMatrix are typealiases for 1D and 2D DataArray's
##
##############################################################################

typealias AbstractDataVector{T} AbstractDataArray{T, 1}
typealias AbstractDataMatrix{T} AbstractDataArray{T, 2}
typealias DataVector{T} DataArray{T, 1}
typealias DataMatrix{T} DataArray{T, 2}

##############################################################################
##
## DataArray constructors
##
##############################################################################

# Need to redefine inner constructor as outer constuctor
DataArray{T, N}(d::Array{T, N}, m::BitArray{N}) = DataArray{T, N}(d, m)

# Convert an Array into a DataArray w/o NA's
DataArray(d::Array) = DataArray(d, falses(size(d)))

# A no-op constructor
DataArray(d::DataArray) = d

# Convert Array{Bool}'s to BitArray's to save space
DataArray(d::Array, m::Array{Bool}) = DataArray(d, bitpack(m))

# Convert a BitArray into a DataArray w/ NA's
DataArray(d::BitArray, m::BitArray) = DataArray(convert(Array{Bool}, d), m)

# Convert a BitArray into a DataArray w/o NA's
DataArray(d::BitArray) = DataArray(convert(Array{Bool}, d), falses(size(d)))

# Convert a Ranges object into a DataVector
DataArray(r::Ranges) = DataArray([r], falses(length(r)))

# Construct an all-NA DataArray of a specific type
DataArray(t::Type, dims::Integer...) = DataArray(Array(t, dims...),
                                                 trues(dims...))
DataArray{N}(t::Type, dims::NTuple{N,Int}) = DataArray(Array(t, dims...), 
                                                 trues(dims...))

# Wrap a scalar in a DataArray w/ repetition
function DataArray(val::Any, dims::Integer...)
    vals = Array(typeof(val), dims...)
    for i in 1:length(vals)
        vals[i] = val
    end
    DataArray(vals, falses(dims...))
end

# Wrap a scalar in a DataArray w/o repetition
DataArray(val::Any) = DataArray([val], falses(1))

##############################################################################
##
## Initialized constructors
##
##############################################################################

# Initialized constructors with 0's, 1's
for (f, basef) in ((:datazeros, :zeros), (:dataones, :ones))
    @eval begin
        ($f)(dims::Int...) = DataArray(($basef)(dims...), falses(dims...))
        ($f)(t::Type, dims::Int...) = DataArray(($basef)(t, dims...),
                                                falses(dims...))
    end
end

# Initialized constructors with false's or true's
for (f, basef) in ((:datafalses, :falses), (:datatrues, :trues))
    @eval begin
        ($f)(dims::Int...) = DataArray(($basef)(dims...), falses(dims...))
    end
end

##############################################################################
##
## Copying
##
##############################################################################

copy(d::DataArray) = DataArray(copy(d.data), copy(d.na))
deepcopy(d::DataArray) = DataArray(deepcopy(d.data), deepcopy(d.na))
function copy_to(dest::DataArray, src::Any)
    i = 1
    for x in src
        dest[i] = x
        i += 1
    end
    return dest
end

##############################################################################
##
## similar()
##
##############################################################################

similar(d::DataArray) = d

function similar{T}(d::DataArray{T}, dims::Int...)
    DataArray(Array(T, dims...), trues(dims...))
end

function similar{T}(d::DataArray{T}, dims::Dims)
    DataArray(Array(T, dims), trues(dims))
end

##############################################################################
##
## Size information
##
##############################################################################

size(d::DataArray) = size(d.data)
ndims(d::DataArray) = ndims(d.data)
length(d::DataArray) = length(d.data)
endof(d::DataArray) = endof(d.data)
eltype{T, N}(d::DataArray{T, N}) = T

##############################################################################
##
## Generic Strategies for dealing with NA's
##
## Editing Functions:
##
## * failNA: Operations should die on the presence of NA's.
## * removeNA: What was once called FILTER.
## * replaceNA: What was once called REPLACE.
##
## Iterator Functions:
##
## * each_failNA: Operations should die on the presence of NA's.
## * each_removeNA: What was once called FILTER.
## * each_replaceNA: What was once called REPLACE.
##
## v = failNA(dv)
##
## for v in each_failNA(dv)
##     do_something_with_value(v)
## end
##
##############################################################################

function failNA(da::DataArray)
    if anyna(da)
        throw(NAException())
    else
        return copy(da.data)
    end
end

# Can do strange things on DataArray of rank > 1
function removeNA(da::DataArray)
    return copy(da.data[!da.na])
end

function replaceNA(da::DataArray, replacement_val::Any)
    res = copy(da.data)
    for i in 1:length(da)
        if da.na[i]
            res[i] = replacement_val
        end
    end
    return res
end

replaceNA(replacement_val::Any) = x -> replaceNA(x, replacement_val)

# TODO: Re-implement these methods for PooledDataArray's
function failNA{T}(da::AbstractDataArray{T})
    if anyna(da)
        throw(NAException())
    else
        res = Array(T, size(da))
        for i in 1:length(da)
            res[i] = da[i]
        end
        return res
    end
end

# TODO: Figure out how to make this work for Array's
function removeNA{T}(da::AbstractDataVector{T})
    n = length(da)
    res = Array(T, n)
    total = 0
    for i in 1:n
        if !isna(da[i])
            total += 1
            res[total] = convert(T, da[i])
        end
    end
    return res[1:total]
end

function removeNA(a::AbstractArray)
    return a
end

function replaceNA{S, T}(da::AbstractDataArray{S}, replacement_val::T)
    res = Array(S, size(da))
    for i in 1:length(da)
        if isna(da[i])
            res[i] = replacement_val
        else
            res[i] = da[i]
        end
    end
    return res
end

##
## NA-aware iterators
##

type EachFailNA{T}
    da::AbstractDataArray{T}
end
each_failNA{T}(da::AbstractDataArray{T}) = EachFailNA(da)
start(itr::EachFailNA) = 1
function done(itr::EachFailNA, ind::Integer)
    return ind > length(itr.da)
end
function next(itr::EachFailNA, ind::Integer)
    if isna(itr.da[ind])
        throw(NAException())
    else
        (itr.da[ind], ind + 1)
    end
end

type EachRemoveNA{T}
    da::AbstractDataArray{T}
end
each_removeNA{T}(da::AbstractDataArray{T}) = EachRemoveNA(da)
start(itr::EachRemoveNA) = 1
function done(itr::EachRemoveNA, ind::Integer)
    return ind > length(itr.da)
end
function next(itr::EachRemoveNA, ind::Integer)
    while ind <= length(itr.da) && isna(itr.da[ind])
        ind += 1
    end
    (itr.da[ind], ind + 1)
end

type EachReplaceNA{S, T}
    da::AbstractDataArray{S}
    replacement_val::T
end
function each_replaceNA(da::AbstractDataArray, val::Any)
    EachReplaceNA(da, convert(eltype(da), val))
end
function each_replaceNA(val::Any)
    x -> each_replaceNA(x, val)
end
start(itr::EachReplaceNA) = 1
function done(itr::EachReplaceNA, ind::Integer)
    return ind > length(itr.da)
end
function next(itr::EachReplaceNA, ind::Integer)
    if isna(itr.da[ind])
        (itr.replacement_val, ind + 1)
    else
        (itr.da[ind], ind + 1)
    end
end

##############################################################################
##
## getindex()
##
##############################################################################

# v[dv]
function getindex(x::Vector, inds::AbstractDataVector{Bool})
    return x[find(replaceNA(inds, false))]
end
function getindex{S, T}(x::Vector{S}, inds::AbstractDataVector{T})
    return x[removeNA(inds)]
end

# d[SingleItemIndex]
function getindex(d::DataArray, i::Real)
	if d.na[i]
		return NA
	else
		return d.data[i]
	end
end

# d[MultiItemIndex]
# TODO: Return SubDataArray
function getindex(d::DataArray, inds::AbstractDataVector{Bool})
    inds = find(replaceNA(inds, false))
    return d[inds]
end
# TODO: Return SubDataArray
function getindex(d::DataArray, inds::AbstractDataVector)
    inds = removeNA(inds)
    return d[inds]
end

# TODO: Return SubDataArray
# TODO: Make inds::AbstractVector
## # There are two definitions in order to remove ambiguity warnings
getindex{T<:Number,N}(d::DataArray{T,N}, inds::Union(BitVector, Vector{Bool})) = DataArray(d.data[inds], d.na[inds])
getindex{T<:Number,N}(d::DataArray{T,N}, inds::Union(Vector, Ranges, BitVector)) = DataArray(d.data[inds], d.na[inds])
function getindex(d::DataArray, inds::Union(BitVector, Vector{Bool}))
    res = similar(d, sum(inds))
    j = 1
    for i in 1:length(inds)
        if inds[i]
            if !d.na[i]
                res[j] = d.data[i]
            end
            j += 1
        end
    end
    res
end

function getindex(d::DataArray, inds::Union(Vector, Ranges))
    res = similar(d, length(inds))
    for i in 1:length(inds)
        ix = inds[i]
        if !d.na[ix]
            res[i] = d.data[ix]
        end
    end
    res
end

# TODO: Return SubDataArray
# TODO: Make inds::AbstractVector
## # The following assumes that T<:Number won't have #undefs
## # There are two definitions in order to remove ambiguity warnings
getindex{T<:Number,N}(d::DataArray{T,N}, inds::Union(BitVector, Vector{Bool})) = DataArray(d.data[inds], d.na[inds])
getindex{T<:Number,N}(d::DataArray{T,N}, inds::Union(Vector, Ranges, BitVector)) = DataArray(d.data[inds], d.na[inds])

# dm[SingleItemIndex, SingleItemIndex)
function getindex(d::DataMatrix, i::Real, j::Real)
    if d.na[i, j]
        return NA
    else
        return d.data[i, j]
    end
end

# dm[SingleItemIndex, MultiItemIndex]
function getindex(x::DataMatrix, i::Real, col_inds::AbstractDataVector{Bool})
    getindex(x, i, find(replaceNA(col_inds, false)))
end
function getindex(x::DataMatrix, i::Real, col_inds::AbstractDataVector)
    getindex(x, i, removeNA(col_inds))
end
# TODO: Make inds::AbstractVector
function getindex(x::DataMatrix,
             i::Real,
             col_inds::Union(Vector, BitVector, Ranges))
    DataArray(x.data[i, col_inds], x.na[i, col_inds])
end

# dm[MultiItemIndex, SingleItemIndex]
function getindex(x::DataMatrix, row_inds::AbstractDataVector{Bool}, j::Real)
    getindex(x, find(replaceNA(row_inds, false)), j)
end
function getindex(x::DataMatrix, row_inds::AbstractVector, j::Real)
    getindex(x, removeNA(row_inds), j)
end
# TODO: Make inds::AbstractVector
function getindex(x::DataMatrix,
             row_inds::Union(Vector, BitVector, Ranges),
             j::Real)
    DataArray(x.data[row_inds, j], x.na[row_inds, j])
end

# dm[MultiItemIndex, MultiItemIndex]
function getindex(x::DataMatrix,
             row_inds::AbstractDataVector{Bool},
             col_inds::AbstractDataVector{Bool})
    getindex(x, find(replaceNA(row_inds, false)), find(replaceNA(col_inds, false)))
end
function getindex(x::DataMatrix,
             row_inds::AbstractDataVector{Bool},
             col_inds::AbstractDataVector)
    getindex(x, find(replaceNA(row_inds, false)), removeNA(col_inds))
end
# TODO: Make inds::AbstractVector
function getindex(x::DataMatrix,
             row_inds::AbstractDataVector{Bool},
             col_inds::Union(Vector, BitVector, Ranges))
    getindex(x, find(replaceNA(row_inds, false)), col_inds)
end
function getindex(x::DataMatrix,
             row_inds::AbstractDataVector,
             col_inds::AbstractDataVector{Bool})
    getindex(x, removeNA(row_inds), find(replaceNA(col_inds, false)))
end
function getindex(x::DataMatrix,
             row_inds::AbstractDataVector,
             col_inds::AbstractDataVector)
    getindex(x, removeNA(row_inds), removeNA(col_inds))
end

# TODO: Make inds::AbstractVector
function getindex(x::DataMatrix,
             row_inds::AbstractDataVector,
             col_inds::Union(Vector, BitVector, Ranges))
    getindex(x, removeNA(row_inds), col_inds)
end

# TODO: Make inds::AbstractVector
function getindex(x::DataMatrix,
             row_inds::Union(Vector, BitVector, Ranges),
             col_inds::AbstractDataVector{Bool})
    getindex(x, row_inds, find(replaceNA(col_inds, false)))
end

# TODO: Make inds::AbstractVector
function getindex(x::DataMatrix,
             row_inds::Union(Vector, BitVector, Ranges),
             col_inds::AbstractDataVector)
    getindex(x, row_inds, removeNA(col_inds))
end

# TODO: Make inds::AbstractVector
function getindex(x::DataMatrix,
             row_inds::Union(Vector, BitVector, Ranges),
             col_inds::Union(Vector, BitVector, Ranges))
    DataArray(x.data[row_inds, col_inds], x.na[row_inds, col_inds])
end

##############################################################################
##
## setindex!()
##
##############################################################################

# d[SingleItemIndex] = NA
function setindex!(da::DataArray, val::NAtype, i::Real)
	da.na[i] = true
end

# d[SingleItemIndex] = Single Item
function setindex!(da::DataArray, val::Any, i::Real)
	da.data[i] = val
	da.na[i] = false
end

# d[MultiIndex] = NA
function setindex!(da::DataArray{NAtype}, val::NAtype, inds::AbstractVector{Bool})
    throw(ArgumentError("DataArray{NAtype} is incoherent"))
end
function setindex!(da::DataArray{NAtype}, val::NAtype, inds::AbstractVector)
    throw(ArgumentError("DataArray{NAtype} is incoherent"))
end
function setindex!(da::DataArray, val::NAtype, inds::AbstractVector{Bool})
    da.na[find(inds)] = true
    return NA
end
function setindex!(da::DataArray, val::NAtype, inds::AbstractVector)
    da.na[inds] = true
    return NA
end

# d[MultiIndex] = Multiple Values
function setindex!(da::AbstractDataArray,
                vals::AbstractVector,
                inds::AbstractVector{Bool})
    setindex!(da, vals, find(inds))
end
function setindex!(da::AbstractDataArray,
                vals::AbstractVector,
                inds::AbstractVector)
    for (val, ind) in zip(vals, inds)
        da[ind] = val
    end
    return vals
end

# x[MultiIndex] = Single Item
function setindex!{T}(da::AbstractDataArray{T},
                   val::Union(Number, String, T),
                   inds::AbstractVector{Bool})
    setindex!(da, val, find(inds))
end
function setindex!{T}(da::AbstractDataArray{T},
                   val::Union(Number, String, T),
                   inds::AbstractVector)
    val = convert(T, val)
    for ind in inds
        da[ind] = val
    end
    return val
end
function setindex!(da::AbstractDataArray,
                val::Any,
                inds::AbstractVector{Bool})
    setindex!(da, val, find(inds))
end
function setindex!{T}(da::AbstractDataArray{T},
                   val::Any,
                   inds::AbstractVector)
    val = convert(T, val)
    for ind in inds
        da[ind] = val
    end
    return val
end

# dm[SingleItemIndex, SingleItemIndex] = NA
function setindex!(dm::DataMatrix, val::NAtype, i::Real, j::Real)
    dm.na[i, j] = true
    return NA
end

# dm[SingleItemIndex, SingleItemIndex] = Single Item
function setindex!(dm::DataMatrix, val::Any, i::Real, j::Real)
    dm.data[i, j] = val
    dm.na[i, j] = false
    return val
end

# dm[MultiItemIndex, SingleItemIndex] = NA
function setindex!(dm::DataMatrix,
                val::NAtype,
                row_inds::Union(Vector, BitVector, Ranges),
                j::Real)
    dm.na[row_inds, j] = true
    return NA
end

# dm[MultiItemIndex, SingleItemIndex] = Multiple Items
function setindex!{S, T}(dm::DataMatrix{S},
                      vals::Vector{T},
                      row_inds::Union(Vector, BitVector, Ranges),
                      j::Real)
    dm.data[row_inds, j] = vals
    dm.na[row_inds, j] = false
    return val
end

# dm[MultiItemIndex, SingleItemIndex] = Single Item
function setindex!(dm::DataMatrix,
                val::Any,
                row_inds::Union(Vector, BitVector, Ranges),
                j::Real)
    dm.data[row_inds, j] = val
    dm.na[row_inds, j] = false
    return val
end

# dm[SingleItemIndex, MultiItemIndex] = NA
function setindex!(dm::DataMatrix,
                val::NAtype,
                i::Real,
                col_inds::Union(Vector, BitVector, Ranges))
    dm.na[i, col_inds] = true
    return NA
end

# dm[SingleItemIndex, MultiItemIndex] = Multiple Items
function setindex!{S, T}(dm::DataMatrix{S},
                      vals::Vector{T},
                      i::Real,
                      col_inds::Union(Vector, BitVector, Ranges))
    dm.data[i, col_inds] = vals
    dm.na[i, col_inds] = false
    return val
end

# dm[SingleItemIndex, MultiItemIndex] = Single Item
function setindex!(dm::DataMatrix,
                val::Any,
                i::Real,
                col_inds::Union(Vector, BitVector, Ranges))
    dm.data[i, col_inds] = val
    dm.na[i, col_inds] = false
    return val
end

# dm[MultiItemIndex, MultiItemIndex] = NA
function setindex!(dm::DataMatrix,
                val::NAtype,
                row_inds::Union(Vector, BitVector, Ranges),
                col_inds::Union(Vector, BitVector, Ranges))
    dm.na[row_inds, col_inds] = true
    return NA
end

# dm[MultiIndex, MultiIndex] = Multiple Items
function setindex!{S, T}(dm::DataMatrix{S},
                      vals::Vector{T},
                      row_inds::Union(Vector, BitVector, Ranges),
                      col_inds::Union(Vector, BitVector, Ranges))
    dm.data[row_inds, col_inds] = vals
    dm.na[row_inds, col_inds] = false
    return val
end

# dm[MultiItemIndex, MultiItemIndex] = Single Item
function setindex!(dm::DataMatrix,
                val::Any,
                row_inds::Union(Vector, BitVector, Ranges),
                col_inds::Union(Vector, BitVector, Ranges))
    dm.data[row_inds, col_inds] = val
    dm.na[row_inds, col_inds] = false
    return val
end

##############################################################################
##
## Predicates
##
##############################################################################

isna(da::DataArray) = copy(da.na)
isnan(da::DataArray) = DataArray(isnan(da.data), copy(da.na))
isfinite(da::DataArray) = DataArray(isfinite(da.data), copy(da.na))
anyna(d::AbstractDataArray) = any(isna, d)
allna(d::AbstractDataArray) = allp(isna, d)

isna(a::AbstractArray) = falses(size(a))
anyna(a::AbstractArray) = false
allna(a::AbstractArray) = false

##############################################################################
##
## Generic iteration over AbstractDataArray's
##
##############################################################################

start(x::AbstractDataArray) = 1

function next(x::AbstractDataArray, state::Integer)
    return (x[state], state + 1)
end

function done(x::AbstractDataArray, state::Integer)
    return state > length(x)
end

##############################################################################
##
## Promotion rules
##
##############################################################################

## promote_rule{T, T}(::Type{AbstractDataArray{T}},
##                    ::Type{T}) = promote_rule(T, T)
## promote_rule{S, T}(::Type{AbstractDataArray{S}},
##                    ::Type{T}) = promote_rule(S, T)
## promote_rule{T}(::Type{AbstractDataArray{T}}, ::Type{T}) = T

##############################################################################
##
## Conversion rules
##
##############################################################################

function convert{N}(::Type{BitArray{N}}, d::DataArray{BitArray{N}, N})
    throw(ArgumentError("Can't convert to BitArray"))
end

function convert{T, N}(::Type{BitArray{N}}, d::DataArray{T, N})
    throw(ArgumentError("Can't convert to BitArray"))
end

function convert{T, N}(::Type{Array{T, N}}, x::DataArray{T, N})
    if anyna(x)
        err = "Cannot convert DataArray with NA's to base type"
        throw(NAException(err))
    else
        return x.data
    end
end

function convert{S, T, N}(::Type{Array{S, N}}, x::DataArray{T, N})
    if anyna(x)
        err = "Cannot convert DataArray with NA's to desired type"
        throw(NAException(err))
    else
        return convert(S, x.data)
    end
end

##############################################################################
##
## Conversion convenience functions
##
##############################################################################

for f in (:int, :float, :bool)
    @eval begin
        function ($f){T}(da::DataArray{T})
            if anyna(da)
                err = "Cannot convert DataArray with NA's to desired type"
                throw(NAException(err))
            else
                ($f)(da.data)
            end
        end
    end
end
for (f, basef) in ((:dataint, :int), (:datafloat, :float64), (:databool, :bool))
    @eval begin
        function ($f){T}(a::Array{T})
            DataArray(($basef)(a))
        end
        function ($f){T}(da::DataArray{T})
            DataArray(($basef)(da.data), copy(da.na))
        end
    end
end

##############################################################################
##
## padNA
##
##############################################################################

function padNA(dv::AbstractDataVector, front::Int, back::Int)
    n = length(dv)
    res = similar(dv, front + n + back)
    for i in 1:n
        res[i + front] = dv[i]
    end
    return res
end

##############################################################################
##
## Conversion
##
##############################################################################

function vector(adv::AbstractDataVector, t::Type, replacement_val::Any)
    n = length(adv)
    res = Array(t, n)
    for i in 1:n
        if isna(adv[i])
            res[i] = replacement_val
        else
            res[i] = adv[i]
        end
    end
    return res
end
function vector(adv::AbstractDataVector, t::Type)
    n = length(adv)
    res = Array(t, n)
    for i in 1:n
        res[i] = adv[i]
    end
    return res
end
vector{T}(adv::AbstractDataVector{T}) = vector(adv, T)

function matrix(adm::AbstractDataMatrix, t::Type, replacement_val::Any)
    n, p = size(adm)
    res = Array(t, n, p)
    for i in 1:n
        for j in 1:p
            if isna(adm[i, j])
                res[i, j] = replacement_val
            else
                res[i, j] = adm[i, j]
            end
        end
    end
    return res
end
function matrix(adm::AbstractDataMatrix, t::Type)
    n, p = size(adm)
    res = Array(t, n, p)
    for i in 1:n
        for j in 1:p
            res[i, j] = adm[i, j]
        end
    end
    return res
end
matrix{T}(adm::AbstractDataMatrix{T}) = matrix(adm, T)

##############################################################################
##
## Hashing
##
## Make sure this agrees with is_equals()
##
##############################################################################

function hash(a::AbstractDataArray)
    h = hash(size(a)) + 1
    for i in 1:length(a)
        h = bitmix(h, int(hash(a[i])))
    end
    return uint(h)
end
