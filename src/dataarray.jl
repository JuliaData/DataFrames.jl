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
            error("Data and missingness arrays must be the same size")
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

# Wrap a scalar in a DataArray
function DataArray(val::Any, dims::Integer...)
    vals = Array(typeof(val), dims...)
    for i in 1:length(vals)
        vals[i] = val
    end
    DataArray(vals, falses(dims...))
end
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
# TODO: copy_to()

##############################################################################
##
## similar()
##
##############################################################################

similar{T}(d::DataArray{T}) = d

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

function failNA(dv::DataVector)
    n = length(dv)
    for i in 1:n
        if dv.na[i]
            error("Failing after encountering an NA")
        end
    end
    return copy(dv.data)
end

function removeNA(dv::DataVector)
    return copy(dv.data[!dv.na])
end

function replaceNA(dv::DataVector, replacement_val::Any)
    n = length(dv)
    res = copy(dv.data)
    for i in 1:n
        if dv.na[i]
            res[i] = replacement_val
        end
    end
    return res
end

# TODO: Re-implement these methods more efficently
function failNA{T}(dv::AbstractDataVector{T})
    n = length(dv)
    for i in 1:n
        if isna(dv[i])
            error("Failing after encountering an NA")
        end
    end
    return convert(Vector{T}, [x::T for x in dv])
end

function removeNA{T}(dv::AbstractDataVector{T})
    return convert(Vector{T}, [x::T for x in dv[!isna(dv)]])
end

function replaceNA{S, T}(dv::AbstractDataVector{S}, replacement_val::T)
    n = length(dv)
    res = Array(S, n)
    for i in 1:n
        if isna(dv[i])
            res[i] = replacement_val
        else
            res[i] = dv[i]
        end
    end
    return res
end

# TODO: Remove this?
vector(dv::AbstractDataVector) = failNA(dv)

type EachFailNA{T}
    dv::AbstractDataVector{T}
end
each_failNA{T}(dv::AbstractDataVector{T}) = EachFailNA(dv)
start(itr::EachFailNA) = 1
function done(itr::EachFailNA, ind::Int)
    return ind > length(itr.dv)
end
function next(itr::EachFailNA, ind::Int)
    if isna(itr.dv[ind])
        error("NA's encountered. Failing...")
    else
        (itr.dv[ind], ind + 1)
    end
end

type EachRemoveNA{T}
    dv::AbstractDataVector{T}
end
each_removeNA{T}(dv::AbstractDataVector{T}) = EachRemoveNA(dv)
start(itr::EachRemoveNA) = 1
function done(itr::EachRemoveNA, ind::Int)
    return ind > length(itr.dv)
end
function next(itr::EachRemoveNA, ind::Int)
    while ind <= length(itr.dv) && isna(itr.dv[ind])
        ind += 1
    end
    (itr.dv[ind], ind + 1)
end

type EachReplaceNA{T}
    dv::AbstractDataVector{T}
    replacement_val::T
end
each_replaceNA{T}(dv::AbstractDataVector{T}, v::T) = EachReplaceNA(dv, v)
start(itr::EachReplaceNA) = 1
function done(itr::EachReplaceNA, ind::Int)
    return ind > length(itr.dv)
end
function next(itr::EachReplaceNA, ind::Int)
    if isna(itr.dv[ind])
        (itr.replacement_val, ind + 1)
    else
        (itr.dv[ind], ind + 1)
    end
end

##############################################################################
##
## ref()
##
##############################################################################

# v[dv]
function ref(x::Vector, inds::AbstractDataVector{Bool})
    return x[find(replaceNA(inds, false))]
end
function ref{S, T}(x::Vector{S}, inds::AbstractDataVector{T})
    return x[removeNA(inds)]
end

# d[SingleItemIndex]
function ref(d::DataArray, i::Real)
	if d.na[i]
		return NA
	else
		return d.data[i]
	end
end

# d[MultiItemIndex]
function ref(d::DataArray, inds::AbstractDataVector{Bool})
    inds = find(replaceNA(inds, false))
    return DataArray(d.data[inds], d.na[inds])
end
function ref(d::DataArray, inds::AbstractDataVector)
    inds = removeNA(inds)
    return DataArray(d.data[inds], d.na[inds])
end
function ref(d::DataArray, inds::Union(Vector, BitVector, Ranges))
    return DataArray(d.data[inds], d.na[inds])
end

# dm[SingleItemIndex, SingleItemIndex)
function ref(d::DataMatrix, i::Real, j::Real)
    if d.na[i, j]
        return NA
    else
        return d.data[i, j]
    end
end

# dm[SingleItemIndex, MultiItemIndex]
function ref(x::DataMatrix, i::Real, col_inds::AbstractDataVector{Bool})
    ref(x, i, find(replaceNA(col_inds, false)))
end
function ref(x::DataMatrix, i::Real, col_inds::AbstractDataVector)
    ref(x, i, removeNA(col_inds))
end
function ref(x::DataMatrix,
             i::Real,
             col_inds::Union(Vector, BitVector, Ranges))
    DataArray(x.data[i, col_inds], x.na[i, col_inds])
end

# dm[MultiItemIndex, SingleItemIndex]
function ref(x::DataMatrix, row_inds::AbstractDataVector{Bool}, j::Real)
    ref(x, find(replaceNA(row_inds, false)), j)
end
function ref(x::DataMatrix, row_inds::AbstractVector, j::Real)
    ref(x, removeNA(row_inds), j)
end
function ref(x::DataMatrix,
             row_inds::Union(Vector, BitVector, Ranges),
             j::Real)
    DataArray(x.data[row_inds, j], x.na[row_inds, j])
end

# dm[MultiItemIndex, MultiItemIndex]
function ref(x::DataMatrix,
             row_inds::AbstractDataVector{Bool},
             col_inds::AbstractDataVector{Bool})
    ref(x, find(replaceNA(row_inds, false)), find(replaceNA(col_inds, false)))
end
function ref(x::DataMatrix,
             row_inds::AbstractDataVector{Bool},
             col_inds::AbstractDataVector)
    ref(x, find(replaceNA(row_inds, false)), removeNA(col_inds))
end
function ref(x::DataMatrix,
             row_inds::AbstractDataVector{Bool},
             col_inds::Union(Vector, BitVector, Ranges))
    ref(x, find(replaceNA(row_inds, false)), col_inds)
end
function ref(x::DataMatrix,
             row_inds::AbstractDataVector,
             col_inds::AbstractDataVector{Bool})
    ref(x, removeNA(row_inds), find(replaceNA(col_inds, false)))
end
function ref(x::DataMatrix,
             row_inds::AbstractDataVector,
             col_inds::AbstractDataVector)
    ref(x, removeNA(row_inds), removeNA(col_inds))
end
function ref(x::DataMatrix,
             row_inds::AbstractDataVector,
             col_inds::Union(Vector, BitVector, Ranges))
    ref(x, removeNA(row_inds), col_inds)
end
function ref(x::DataMatrix,
             row_inds::Union(Vector, BitVector, Ranges),
             col_inds::AbstractDataVector{Bool})
    ref(x, row_inds, find(replaceNA(col_inds, false)))
end
function ref(x::DataMatrix,
             row_inds::Union(Vector, BitVector, Ranges),
             col_inds::AbstractDataVector)
    ref(x, row_inds, removeNA(col_inds))
end
function ref(x::DataMatrix,
             row_inds::Union(Vector, BitVector, Ranges),
             col_inds::Union(Vector, BitVector, Ranges))
    DataArray(x.data[row_inds, col_inds], x.na[row_inds, col_inds])
end

##############################################################################
##
## assign()
##
##############################################################################

# d[SingleItemIndex] = NA
function assign(d::DataArray, val::NAtype, i::Real)
	d.na[i] = true
end
# d[SingleItemIndex] = Single Item
function assign(d::DataArray, val::Any, i::Real)
	d.data[i] = val
	d.na[i] = false
end

# d[MultiIndex] = NA
function assign(d::DataArray{NAtype}, val::NAtype, inds::AbstractVector{Bool})
    error("Don't use DataVector{NAtype}'s")
end
function assign(d::DataArray{NAtype}, val::NAtype, inds::AbstractVector)
    error("Don't use DataVector{NAtype}'s")
end
function assign(d::DataArray, val::NAtype, inds::AbstractVector{Bool})
    d.na[find(inds)] = true
    return NA
end
function assign(d::DataArray, val::NAtype, inds::AbstractVector)
    d.na[inds] = true
    return NA
end

# d[MultiIndex] = Multiple Values
function assign(d::AbstractDataArray,
                vals::AbstractVector,
                inds::AbstractVector{Bool})
    assign(d, vals, find(inds))
end
function assign(d::AbstractDataArray,
                vals::AbstractVector,
                inds::AbstractVector)
    for (val, ind) in zip(vals, inds)
        d[ind] = val
    end
    return vals
end

# x[MultiIndex] = Single Item
# Single item can be a Number, String or the eltype of the AbstractDataVector
# Should be val::Union(Number, String, T), but that doesn't work
function assign{T}(x::AbstractDataArray{T},
                   val::Number,
                   inds::AbstractVector{Bool})
    assign(x, val, find(inds))
end
function assign{T}(x::AbstractDataArray{T},
                   val::Number,
                   inds::AbstractVector)
    val = convert(eltype(x), val)
    for ind in inds
        x[ind] = val
    end
    return val
end
function assign{T}(x::AbstractDataArray{T},
                   val::String,
                   inds::AbstractVector{Bool})
    assign(x, val, find(inds))
end
function assign{T}(x::AbstractDataArray{T},
                   val::String,
                   inds::AbstractVector)
    val = convert(eltype(x), val)
    for ind in inds
        x[ind] = val
    end
    return val
end
function assign{T <: Number}(x::AbstractDataArray{T},
                   val::T,
                   inds::AbstractVector{Bool})
    assign(x, val, find(inds))
end
function assign{T <: Number}(x::AbstractDataArray{T},
                   val::T,
                   inds::AbstractVector)
    val = convert(eltype(x), val)
    for ind in inds
        x[ind] = val
    end
    return val
end
function assign{T <: String}(x::AbstractDataArray{T},
                   val::T,
                   inds::AbstractVector{Bool})
    assign(x, val, find(inds))
end
function assign{T <: String}(x::AbstractDataArray{T},
                   val::T,
                   inds::AbstractVector)
    val = convert(eltype(x), val)
    for ind in inds
        x[ind] = val
    end
    return val
end
function assign(x::AbstractDataArray,
                val::Any,
                inds::AbstractVector{Bool})
    assign(x, val, find(inds))
end
function assign(x::AbstractDataArray,
                val::Any,
                inds::AbstractVector)
    val = convert(eltype(x), val)
    for ind in inds
        x[ind] = val
    end
    return val
end

# dm[SingleItemIndex, SingleItemIndex] = NA
function assign(x::DataMatrix, val::NAtype, i::Real, j::Real)
    x.na[i, j] = true
    return NA
end
# dm[SingleItemIndex, SingleItemIndex] = Single Item
function assign(x::DataMatrix, val::Any, i::Real, j::Real)
    x.data[i, j] = val
    x.na[i, j] = false
    return val
end

# dm[MultiItemIndex, SingleItemIndex] = NA
function assign(x::DataMatrix,
                val::NAtype,
                row_inds::Union(Vector, BitVector, Ranges),
                j::Real)
    x.na[row_inds, j] = true
    return NA
end
# dm[MultiItemIndex, SingleItemIndex] = Multiple Items
function assign{S, T}(x::DataMatrix{S},
                      vals::Vector{T},
                      row_inds::Union(Vector, BitVector, Ranges),
                      j::Real)
    x.data[row_inds, j] = vals
    x.na[row_inds, j] = false
    return val
end
# dm[MultiItemIndex, SingleItemIndex] = Single Item
function assign(x::DataMatrix,
                val::Any,
                row_inds::Union(Vector, BitVector, Ranges),
                j::Real)
    x.data[row_inds, j] = val
    x.na[row_inds, j] = false
    return val
end

# dm[SingleItemIndex, MultiItemIndex] = NA
function assign(x::DataMatrix,
                val::NAtype,
                i::Real,
                col_inds::Union(Vector, BitVector, Ranges))
    x.na[i, col_inds] = true
    return NA
end
# dm[SingleItemIndex, MultiItemIndex] = Multiple Items
function assign{S, T}(x::DataMatrix{S},
                      vals::Vector{T},
                      i::Real,
                      col_inds::Union(Vector, BitVector, Ranges))
    x.data[i, col_inds] = vals
    x.na[i, col_inds] = false
    return val
end
# dm[SingleItemIndex, MultiItemIndex] = Single Item
function assign(x::DataMatrix,
                val::Any,
                i::Real,
                col_inds::Union(Vector, BitVector, Ranges))
    x.data[i, col_inds] = val
    x.na[i, col_inds] = false
    return val
end

# dm[MultiItemIndex, MultiItemIndex] = NA
function assign(x::DataMatrix,
                val::NAtype,
                row_inds::Union(Vector, BitVector, Ranges),
                col_inds::Union(Vector, BitVector, Ranges))
    x.na[row_inds, col_inds] = true
    return NA
end
# dm[MultiIndex, MultiIndex] = Multiple Items
function assign{S, T}(x::DataMatrix{S},
                      vals::Vector{T},
                      row_inds::Union(Vector, BitVector, Ranges),
                      col_inds::Union(Vector, BitVector, Ranges))
    x.data[row_inds, col_inds] = vals
    x.na[row_inds, col_inds] = false
    return val
end
# dm[MultiItemIndex, MultiItemIndex] = Single Item
function assign(x::DataMatrix,
                val::Any,
                row_inds::Union(Vector, BitVector, Ranges),
                col_inds::Union(Vector, BitVector, Ranges))
    x.data[row_inds, col_inds] = val
    x.na[row_inds, col_inds] = false
    return val
end

##############################################################################
##
## Predicates
##
##############################################################################

isna(d::DataArray) = copy(d.na)
isna(x::AbstractArray) = falses(size(x))

isnan(d::DataArray) = DataArray(isnan(dv.data), copy(dv.na))
isfinite(dv::DataArray) = DataArray(isfinite(dv.data), copy(dv.na))

function any_na(d::AbstractDataArray)
    for i in 1:length(d)
        if isna(d[i])
            return true
        end
    end
    return false
end

##############################################################################
##
## Generic iteration over AbstractDataArray's
##
##############################################################################

start(x::AbstractDataArray) = 1
function next(x::AbstractDataArray, state::Int)
    return (x[state], state + 1)
end
function done(x::AbstractDataArray, state::Int)
    return state > length(x)
end

##############################################################################
##
## Promotion rules
##
##############################################################################

promote_rule{T, T}(::Type{AbstractDataArray{T}},
                   ::Type{T}) = promote_rule(T, T)
promote_rule{S, T}(::Type{AbstractDataArray{S}},
                   ::Type{T}) = promote_rule(S, T)
promote_rule{T}(::Type{AbstractDataArray{T}}, ::Type{T}) = T

##############################################################################
##
## Conversion rules
##
##############################################################################

function convert{N}(::Type{BitArray{N}}, d::DataArray{BitArray{N}, N})
    error("Don't try to convert a DataArray to a BitArray")
end

function convert{T, N}(::Type{BitArray{N}}, d::DataArray{T, N})
    error("Don't try to convert a DataArray to a BitArray")
end

function convert{T, N}(::Type{Array{T, N}}, x::DataArray{T, N})
    if any_na(x)
        err = "Cannot convert DataArray with NA's to base type"
        throw(NAException(err))
    else
        return x.data
    end
end

function convert{S, T, N}(::Type{Array{S, N}}, x::DataArray{T, N})
    if any_na(x)
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
        function ($f){T}(dv::DataArray{T})
            if !any_na(dv)
                ($f)(dv.data)
            else
                error("Conversion impossible with NA's present")
            end
        end
    end
end
for (f, basef) in ((:dataint, :int), (:datafloat, :float64), (:databool, :bool))
    @eval begin
        function ($f){T}(dv::DataArray{T})
            DataArray(($basef)(dv.data), copy(dv.na))
        end
    end
end

##
## padNA
##

function padNA(dv::AbstractDataVector, front::Int, back::Int)
  n = length(dv)
  res = similar(dv, front + n + back)
  for i in 1:n
    res[i + front] = dv[i]
  end
  return res
end

##
## Conversion
##

function vector(adv::AbstractDataVector, t::Type)
    n = length(adv)
    res = Array(t, n)
    for i in 1:n
        res[i] = adv[i]
    end
    return res
end
vector(adv::AbstractDataVector) = vector(adv, eltype(adv))

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
matrix(adm::AbstractDataMatrix) = matrix(adm, eltype(adm))

# TODO: Implement for arbitrary rank tensors
