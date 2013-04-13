##############################################################################
##
## PooledDataArray type definition
##
## An AbstractDataArray with efficient storage when values are repeated. A
## PDA wraps an array of UInt8's, which are used to index into a compressed
## pool of values. NA's are 0's in the UInt8 array.
##
## TODO: Make sure we don't overflow from refs being Uint8
## TODO: Allow ordering of factor levels
## TODO: Add metadata for dummy conversion
##
##############################################################################

type PooledDataArray{T, N} <: AbstractDataArray{T, N}
    refs::Array{POOLED_DATA_VEC_REF_TYPE, N}
    pool::Vector{T}

    function PooledDataArray(rs::Array{POOLED_DATA_VEC_REF_TYPE, N},
                             p::Vector{T})
        # refs mustn't overflow pool
        if max(rs) > prod(size(p))
            error("Reference array points beyond the end of the pool")
        end
        new(rs, p)
    end
end
typealias PooledDataVector{T} PooledDataArray{T, 1}
typealias PooledDataMatrix{T} PooledDataArray{T, 2}

##############################################################################
##
## PooledDataArray constructors
##
##############################################################################

# Echo inner constructor as an outer constructor
function PooledDataArray{T, N}(refs::Array{POOLED_DATA_VEC_REF_TYPE, N},
                               pool::Vector{T})
    PooledDataArray{T, N}(refs, pool)
end


# A no-op constructor
PooledDataArray(d::PooledDataArray) = d

# How do you construct a PooledDataArray from an Array?
# From the same sigs as a DataArray!
#
# Algorithm:
# * Start with:
#   * A null pool
#   * A pre-allocated refs
#   * A hash from T to Int
# * Iterate over d
#   * If value of d in pool already, set the refs accordingly
#   * If value is new, add it to the pool, then set refs
function PooledDataArray{T, N}(d::AbstractArray{T, N}, m::AbstractArray{Bool, N})
    newrefs = Array(POOLED_DATA_VEC_REF_TYPE, size(d))
    #newpool = Array(T, 0)
    poolref = Dict{T, POOLED_DATA_VEC_REF_TYPE}() # Why isn't this a set?

    # Loop through once to fill the poolref dict
    for i = 1:length(d)
        if !m[i]
            poolref[d[i]] = 0
        end
    end

    # Fill positions in poolref
    newpool = sort(keys(poolref))
    i = 1
    for p in newpool
        poolref[p] = i
        i += 1
    end

    # Fill in newrefs
    for i = 1:length(d)
        if m[i]
            newrefs[i] = 0
        else
            newrefs[i] = poolref[d[i]]
        end
    end

    return PooledDataArray(newrefs, newpool)
end

# Allow a pool to be provided by the user
function PooledDataArray{T, N}(d::AbstractArray{T, N},
                               pool::Vector{T},
                               m::AbstractArray{Bool, N})
    if length(pool) > typemax(POOLED_DATA_VEC_REF_TYPE)
        error("Cannot construct a PooledDataVector with such a large pool")
    end

    newrefs = Array(POOLED_DATA_VEC_REF_TYPE, size(d))
    poolref = Dict{T, POOLED_DATA_VEC_REF_TYPE}()

    # loop through once to fill the poolref dict
    for i = 1:length(pool)
        poolref[pool[i]] = i
    end

    # fill in newrefs
    for i = 1:length(d)
        if m[i]
            newrefs[i] = 0
        else
            if has(poolref, d[i])
                newrefs[i] = poolref[d[i]]
            else
                newrefs[i] = 0
            end
        end
    end

    return PooledDataArray(newrefs, pool)
end

# Convert a BitArray to an Array{Bool} w/ specified missingness
function PooledDataArray{N}(d::BitArray{N}, m::AbstractArray{Bool, N})
    PooledDataArray(convert(Array{Bool}, d), m)
end

# Convert a DataArray to a PooledDataArray
PooledDataArray{T}(da::DataArray{T}) = PooledDataArray(da.data, da.na)

# Convert a DataArray to a PooledDataArray
PooledDataArray{T}(da::DataArray{T}, pool::Vector{T}) = PooledDataArray(da.data, pool, da.na)

# Convert a Array{T} to a PooledDataArray
PooledDataArray{T}(a::Array{T}) = PooledDataArray(a, falses(size(a)))

# Convert a BitVector to a Vector{Bool} w/o specified missingness
function PooledDataArray(a::BitArray)
    PooledDataArray(convert(Array{Bool}, a), falses(size(a)))
end

# Explicitly convert Ranges into a PooledDataVector
PooledDataArray(r::Ranges) = PooledDataArray([r], falses(length(r)))

# Construct an all-NA PooledDataVector of a specific type
PooledDataArray(t::Type, dims::Int...) = PooledDataArray(Array(t, dims...), trues(dims...))

# Specify just a vector and a pool
function PooledDataArray{T}(d::Array{T}, pool::Vector{T})
    PooledDataArray(d, pool, falses(size(d)))
end

# Initialized constructors with 0's, 1's
for (f, basef) in ((:pdatazeros, :zeros), (:pdataones, :ones))
    @eval begin
        ($f)(dims::Int...) = PooledDataArray(($basef)(dims...), falses(dims...))
        ($f)(t::Type, dims::Int...) = PooledDataArray(($basef)(t, dims...), falses(dims...))
    end
end

# Initialized constructors with false's or true's
for (f, basef) in ((:pdatafalses, :falses), (:pdatatrues, :trues))
    @eval begin
        ($f)(dims::Int...) = PooledDataArray(($basef)(dims...), falses(dims...))
    end
end

# Super hacked-out constructor: PooledDataVector[1, 2, 2, NA]
function getindex(::Type{PooledDataVector}, vals...)
    # For now, just create a DataVector and then convert it
    # TODO: Rewrite for speed
    PooledDataArray(DataVector[vals...])
end

##############################################################################
##
## Basic size properties of all Data* objects
##
##############################################################################

size(pda::PooledDataArray) = size(pda.refs)
length(pda::PooledDataArray) = length(pda.refs)
endof(pda::PooledDataArray) = endof(pda.refs)

##############################################################################
##
## Copying Data* objects
##
##############################################################################

copy(pda::PooledDataArray) = PooledDataArray(copy(pda.refs),
                                             copy(pda.pool))
# TODO: Implement copy_to()

##############################################################################
##
## Predicates, including the new isna()
##
##############################################################################

function isnan(pda::PooledDataArray)
    PooledDataArray(copy(pda.refs), isnan(pda.pool))
end

function isfinite(pda::PooledDataArray)
    PooledDataArray(copy(pda.refs), isfinite(pda.pool))
end

isna(pda::PooledDataArray) = pda.refs .== 0

##############################################################################
##
## PooledDataArray utilities
##
## TODO: Add methods with these names for DataArray's
##       Decide whether levels() or unique() is primitive. Make the other
##       an alias.
##  Tom: I don't think levels and unique are the same. R doesn't include NA's
##       with levels, but it does with unique. Having these different is
##       useful.
##
##############################################################################

# Convert a PooledDataVector{T} to a DataVector{T}
function values{T}(pda::PooledDataArray{T})
    res = DataArray(T, size(pda)...)
    for i in 1:length(pda)
        r = pda.refs[i]
        if r == 0
            res[i] = NA
        else
            res[i] = pda.pool[r]
        end
    end
    return res
end
DataArray(pda::PooledDataArray) = values(pda)
values(da::DataArray) = copy(da)
values(a::Array) = copy(a)

function unique{T}(x::PooledDataArray{T})
    if any(x.refs .== 0)
        n = length(x.pool)
        d = Array(T, n + 1)
        for i in 1:n
            d[i] = x.pool[i]
        end
        m = falses(n + 1)
        m[n + 1] = true
        return DataArray(d, m)
    else
        return DataArray(copy(x.pool), falses(length(x.pool)))
    end
end
levels{T}(pda::PooledDataArray{T}) = pda.pool

function unique{T}(adv::AbstractDataVector{T})
  values = Dict{Union(T, NAtype), Bool}()
  for i in 1:length(adv)
    values[adv[i]] = true
  end
  unique_values = keys(values)
  res = DataArray(T, length(unique_values))
  for i in 1:length(unique_values)
    res[i] = unique_values[i]
  end
  return res
end
levels{T}(adv::AbstractDataVector{T}) = unique(adv)

get_indices{T}(x::PooledDataArray{T}) = x.refs

function index_to_level{T}(x::PooledDataArray{T})
    d = Dict{POOLED_DATA_VEC_REF_TYPE, T}()
    for i in POOLED_DATA_VEC_REF_CONVERTER(1:length(x.pool))
        d[i] = x.pool[i]
    end
    return d
end

function level_to_index{T}(x::PooledDataArray{T})
    d = Dict{T, POOLED_DATA_VEC_REF_TYPE}()
    for i in POOLED_DATA_VEC_REF_CONVERTER(1:length(x.pool))
        d[x.pool[i]] = i
    end
    d
end

function PooledDataArray{S,N}(x::PooledDataArray{S,N},
                              newpool::Vector{S})
    # QUESTION: should we have a ! version of this? If so, needs renaming?
    tidx = POOLED_DATA_VEC_REF_CONVERTER(findat(x.pool, newpool))
    refs = zeros(POOLED_DATA_VEC_REF_TYPE, length(x))
    for i in 1:length(refs)
        if x.refs[i] != 0 
            refs[i] = tidx[x.refs[i]]
        end
    end
    PooledDataArray(refs, newpool)
end

myunique(x::AbstractVector) = x[sort(unique(findat(x, x)))]  # gets the ordering right
myunique(x::AbstractDataVector) = myunique(removeNA(x))   # gets the ordering right; removes NAs

function set_levels(x::PooledDataArray, newpool::AbstractVector)
    pool = myunique(newpool)
    refs = zeros(POOLED_DATA_VEC_REF_TYPE, length(x))
    tidx = POOLED_DATA_VEC_REF_CONVERTER(findat(newpool, pool))
    tidx[isna(newpool)] = 0
    for i in 1:length(refs)
        if x.refs[i] != 0
            refs[i] = tidx[x.refs[i]]
        end
    end
    return PooledDataArray(refs, pool)
end

function set_levels!{T}(x::PooledDataArray{T}, newpool::AbstractVector{T})
    if newpool == myunique(newpool) # no NAs or duplicates
        x.pool = newpool
        return x
    else
        x.pool = myunique(newpool)
        tidx = POOLED_DATA_VEC_REF_CONVERTER(findat(newpool, x.pool))
        tidx[isna(newpool)] = 0
        for i in 1:length(x.refs)
            if x.refs[i] != 0
                x.refs[i] = tidx[x.refs[i]]
            end
        end
        return x
    end
end

function set_levels(x::PooledDataArray, d::Dict)
    newpool = copy(DataArray(x.pool))
    # An NA in `v` is put in the pool; that will cause it to become NA
    for (k,v) in d
        idx = findin(newpool, [k])
        if length(idx) == 1
            newpool[idx[1]] = v
        end
    end
    set_levels(x, newpool)
end

function set_levels!{T}(x::PooledDataArray{T}, d::Dict{T,T})
    for (k,v) in d
        idx = findin(x.pool, [k])
        if length(idx) == 1
            x.pool[idx[1]] = v
        end
    end
    x
end

function set_levels!{T}(x::PooledDataArray{T}, d::Dict{T,Any}) # this version handles NAs in d's values
    newpool = copy(DataArray(x.pool))
    # An NA in `v` is put in the pool; that will cause it to become NA
    for (k,v) in d
        idx = findin(newpool, [k])
        if length(idx) == 1
            newpool[idx[1]] = v
        end
    end
    set_levels!(x, newpool)
end

reorder(x::PooledDataArray)  = PooledDataArray(x, sort(levels(x)))  # just re-sort the pool

reorder(x::PooledDataArray, y::AbstractVector...) = reorder(mean, x, y...)

reorder(fun::Function, x::PooledDataArray, y::AbstractVector...) =
    reorder(fun, x, DataFrame({y...}))

reverse(x::PooledDataArray) = PooledDataArray(reverse(x.refs), x.pool)

##############################################################################
##
## similar()
##
##############################################################################

similar(pda::PooledDataArray) = pda

function similar(pda::PooledDataArray, dims::Int...)
    PooledDataArray(fill(POOLED_DATA_VEC_REF_CONVERTER(0), dims...), pda.pool)
end

function similar(pda::PooledDataArray, dims::Dims)
    PooledDataArray(fill(POOLED_DATA_VEC_REF_CONVERTER(0), dims), pda.pool)
end

##############################################################################
##
## find()
##
##############################################################################

find(pdv::PooledDataVector{Bool}) = find(values(pdv))

##############################################################################
##
## getindex()
##
##############################################################################

# pda[SingleItemIndex]
function getindex(pda::PooledDataArray, i::Real)
    if pda.refs[i] == 0
        return NA
    else
        return pda.pool[pda.refs[i]]
    end
end

# pda[MultiItemIndex]
function getindex(pda::PooledDataArray, inds::AbstractDataVector{Bool})
    inds = find(replaceNA(inds, false))
    return PooledDataArray(pda.refs[inds], copy(pda.pool))
end
function getindex(pda::PooledDataArray, inds::AbstractDataVector)
    inds = removeNA(inds)
    return PooledDataArray(pda.refs[inds], copy(pda.pool))
end
function getindex(pda::PooledDataArray, inds::Union(Vector, BitVector, Ranges))
    return PooledDataArray(pda.refs[inds], copy(pda.pool))
end

# pdm[SingleItemIndex, SingleItemIndex)
function getindex(pda::PooledDataArray, i::Real, j::Real)
    if pda.refs[i, j] == 0
        return NA
    else
        return pda.pool[pda.refs[i, j]]
    end
end

# pda[SingleItemIndex, MultiItemIndex]
function getindex(pda::PooledDataArray, i::Real, col_inds::AbstractDataVector{Bool})
    getindex(pda, i, find(replaceNA(col_inds, false)))
end
function getindex(pda::PooledDataArray, i::Real, col_inds::AbstractDataVector)
    getindex(pda, i, removeNA(col_inds))
end
# TODO: Make inds::AbstractVector
function getindex(pda::PooledDataArray,
             i::Real,
             col_inds::Union(Vector, BitVector, Ranges))
    error("not yet implemented")
    PooledDataArray(pda.refs[i, col_inds], pda.pool[i, col_inds])
end

# pda[MultiItemIndex, SingleItemIndex]
function getindex(pda::PooledDataArray, row_inds::AbstractDataVector{Bool}, j::Real)
    getindex(pda, find(replaceNA(row_inds, false)), j)
end
function getindex(pda::PooledDataArray, row_inds::AbstractVector, j::Real)
    getindex(pda, removeNA(row_inds), j)
end
# TODO: Make inds::AbstractVector
function getindex(pda::PooledDataArray,
             row_inds::Union(Vector, BitVector, Ranges),
             j::Real)
    error("not yet implemented")
    PooledDataArray(pda.refs[row_inds, j], pda.pool[row_inds, j])
end

# pda[MultiItemIndex, MultiItemIndex]
function getindex(pda::PooledDataArray,
             row_inds::AbstractDataVector{Bool},
             col_inds::AbstractDataVector{Bool})
    getindex(pda, find(replaceNA(row_inds, false)), find(replaceNA(col_inds, false)))
end
function getindex(pda::PooledDataArray,
             row_inds::AbstractDataVector{Bool},
             col_inds::AbstractDataVector)
    getindex(pda, find(replaceNA(row_inds, false)), removeNA(col_inds))
end
# TODO: Make inds::AbstractVector
function getindex(pda::PooledDataArray,
             row_inds::AbstractDataVector{Bool},
             col_inds::Union(Vector, BitVector, Ranges))
    getindex(pda, find(replaceNA(row_inds, false)), col_inds)
end
function getindex(pda::PooledDataArray,
             row_inds::AbstractDataVector,
             col_inds::AbstractDataVector{Bool})
    getindex(pda, removeNA(row_inds), find(replaceNA(col_inds, false)))
end
function getindex(pda::PooledDataArray,
             row_inds::AbstractDataVector,
             col_inds::AbstractDataVector)
    getindex(pda, removeNA(row_inds), removeNA(col_inds))
end
# TODO: Make inds::AbstractVector
function getindex(pda::PooledDataArray,
             row_inds::AbstractDataVector,
             col_inds::Union(Vector, BitVector, Ranges))
    getindex(pda, removeNA(row_inds), col_inds)
end
# TODO: Make inds::AbstractVector
function getindex(pda::PooledDataArray,
             row_inds::Union(Vector, BitVector, Ranges),
             col_inds::AbstractDataVector{Bool})
    getindex(pda, row_inds, find(replaceNA(col_inds, false)))
end
# TODO: Make inds::AbstractVector
function getindex(pda::PooledDataArray,
             row_inds::Union(Vector, BitVector, Ranges),
             col_inds::AbstractDataVector)
    getindex(pda, row_inds, removeNA(col_inds))
end
# TODO: Make inds::AbstractVector
function getindex(pda::PooledDataArray,
             row_inds::Union(Vector, BitVector, Ranges),
             col_inds::Union(Vector, BitVector, Ranges))
    error("not yet implemented")
    PooledDataArray(pda.refs[row_inds, col_inds], pda.pool[row_inds, col_inds])
end

##############################################################################
##
## setindex!() definitions
##
##############################################################################

# x[SingleIndex] = NA
# TODO: Delete values from pool that no longer exist?
# Not a good idea.  Add another function called drop_unused_levels to do this.
# R has the convention that if f is a factor then factor(f) drops unused levels
function setindex!(x::PooledDataArray, val::NAtype, ind::Real)
    x.refs[ind] = 0
    return NA
end

# x[SingleIndex] = Single Item
# TODO: Delete values from pool that no longer exist?
function setindex!(x::PooledDataArray, val::Any, ind::Real)
    val = convert(eltype(x), val)
    pool_idx = findfirst(x.pool, val)
    if pool_idx > 0
        x.refs[ind] = pool_idx
    else
        push!(x.pool, val)
        x.refs[ind] = length(x.pool)
    end
    return val
end

# x[MultiIndex] = NA
# TODO: Find a way to delete the next four methods
function setindex!(x::PooledDataArray{NAtype},
                val::NAtype,
                inds::AbstractVector{Bool})
    error("Don't use PooledDataVector{NAtype}'s")
end
function setindex!(x::PooledDataArray{NAtype},
                val::NAtype,
                inds::AbstractVector)
    error("Don't use PooledDataVector{NAtype}'s")
end
function setindex!(x::PooledDataArray, val::NAtype, inds::AbstractVector{Bool})
    inds = find(inds)
    x.refs[inds] = 0
    return NA
end
function setindex!(x::PooledDataArray, val::NAtype, inds::AbstractVector)
    x.refs[inds] = 0
    return NA
end

# pda[MultiIndex] = Multiple Values
function setindex!(pda::PooledDataArray,
                vals::AbstractVector,
                inds::AbstractVector{Bool})
    setindex!(pda, vals, find(inds))
end
function setindex!(pda::PooledDataArray,
                vals::AbstractVector,
                inds::AbstractVector)
    for (val, ind) in zip(vals, inds)
        pda[ind] = val
    end
    return vals
end

# pda[SingleItemIndex, SingleItemIndex] = NA
function setindex!(pda::PooledDataMatrix, val::NAtype, i::Real, j::Real)
    pda.refs[i, j] = POOLED_DATA_VEC_REF_CONVERTER(0)
    return NA
end
# pda[SingleItemIndex, SingleItemIndex] = Single Item
function assign{T}(pda::PooledDataMatrix{T}, val::Any, i::Real, j::Real)
    val = convert(T, val)
    pool_idx = findfirst(x.pool, val)
    if pool_idx > 0
        pda.refs[i, j] = pool_idx
    else
        push!(pda.pool, val)
        pda.refs[i, j] = length(pda.pool)
    end
    return val
end

##############################################################################
##
## show() and similar methods
##
##############################################################################

function string(x::PooledDataVector)
    tmp = join(x, ", ")
    return "[$tmp]"
end

# # Need setindex!()'s to make this work
# This is broken now because the inner show returns to the outer show.
# function show(io::IO, pda::PooledDataArray)
#     invoke(show, (Any, AbstractArray), io, pda)
#     print(io, "\nlevels: ")
#     print(io, levels(pda))
# end

##############################################################################
##
## Replacement operations
##
##############################################################################

function replace!(x::PooledDataArray{NAtype}, fromval::NAtype, toval::NAtype)
    NA # no-op to deal with warning
end
function replace!(x::PooledDataArray, fromval::NAtype, toval::NAtype)
    NA # no-op to deal with warning
end
function replace!{S, T}(x::PooledDataArray{S}, fromval::T, toval::NAtype)
    fromidx = findfirst(x.pool, fromval)
    if fromidx == 0
        error("can't replace a value not in the pool in a PooledDataVector!")
    end

    x.refs[x.refs .== fromidx] = 0

    return NA
end
function replace!{S, T}(x::PooledDataArray{S}, fromval::NAtype, toval::T)
    toidx = findfirst(x.pool, toval)
    # if toval is in the pool, just do the assignment
    if toidx != 0
        x.refs[x.refs .== 0] = toidx
    else
        # otherwise, toval is new, add it to the pool
        push!(x.pool, toval)
        x.refs[x.refs .== 0] = length(x.pool)
    end

    return toval
end
function replace!{R, S, T}(x::PooledDataArray{R}, fromval::S, toval::T)
    # throw error if fromval isn't in the pool
    fromidx = findfirst(x.pool, fromval)
    if fromidx == 0
        error("can't replace a value not in the pool in a PooledDataArray!")
    end

    # if toval is in the pool too, use that and remove fromval from the pool
    toidx = findfirst(x.pool, toval)
    if toidx != 0
        x.refs[x.refs .== fromidx] = toidx
        #x.pool[fromidx] = None    TODO: what to do here??
    else
        # otherwise, toval is new, swap it in
        x.pool[fromidx] = toval
    end

    return toval
end

##############################################################################
##
## Sorting can use the pool to speed things up
##
##############################################################################

# TODO handle sortperm for non-sorted keys
sortperm(pda::PooledDataArray) = groupsort_indexer(pda)[1]
function sortperm(pda::PooledDataArray)
    if issorted(pda.pool)
        return groupsort_indexer(pda)[1]
    else
        return sortperm(reorder!(copy(pda)))
    end 
end 
        
sortperm(pda::PooledDataArray, ::Sort.Reverse) = reverse(sortperm(pda))
sort(pda::PooledDataArray) = pda[sortperm(pda)]
sort(pda::PooledDataArray, ::Sort.Reverse) = pda[reverse(sortperm(pda))]
type FastPerm{O<:Sort.Ordering,V<:AbstractVector} <: Sort.Ordering
    ord::O
    vec::V
end
FastPerm{O<:Sort.Ordering,V<:AbstractVector}(o::O,v::V) = FastPerm{O,V}(o,v)
sortperm{V}(x::AbstractVector, a::Sort.Algorithm, o::FastPerm{Sort.Forward,V}) = x[sortperm(o.vec)]
sortperm{V}(x::AbstractVector, a::Sort.Algorithm, o::FastPerm{Sort.Reverse,V}) = x[reverse(sortperm(o.vec))]
Perm{O<:Sort.Ordering}(o::O, v::PooledDataVector) = FastPerm(o, v)



##############################################################################
##
## PooledDataVecs: EXPLANATION SHOULD GO HERE
##
##############################################################################


function PooledDataVecs{S,N}(v1::PooledDataArray{S,N},
                             v2::PooledDataArray{S,N})
    pool = sort(unique([v1.pool, v2.pool]))
    tidx1 = POOLED_DATA_VEC_REF_CONVERTER(findat(v1.pool, pool))
    tidx2 = POOLED_DATA_VEC_REF_CONVERTER(findat(v2.pool, pool))
    refs1 = zeros(POOLED_DATA_VEC_REF_TYPE, length(v1))
    refs2 = zeros(POOLED_DATA_VEC_REF_TYPE, length(v2))
    for i in 1:length(refs1)
        if v1.refs[i] != 0
            refs1[i] = tidx1[v1.refs[i]]
        end
    end
    for i in 1:length(refs2)
        if v2.refs[i] != 0
            refs2[i] = tidx2[v2.refs[i]]
        end
    end
    return (PooledDataArray(refs1, pool),
            PooledDataArray(refs2, pool))
end

function PooledDataVecs{S,N}(v1::PooledDataArray{S,N},
                           v2::AbstractArray{S,N})
    return PooledDataVecs(v1,
                          PooledDataArray(v2))
end
function PooledDataVecs{S,N}(v1::PooledDataArray{S,N},
                           v2::AbstractArray{S,N})
    return PooledDataVecs(v1,
                          PooledDataArray(v2))
end
function PooledDataVecs{S,N}(v1::AbstractArray{S,N},
                           v2::PooledDataArray{S,N})
    return PooledDataVecs(PooledDataArray(v1),
                          v2)
end
function PooledDataVecs{S,N}(v1::AbstractArray{S,N},
                           v2::PooledDataArray{S,N})
    return PooledDataVecs(PooledDataArray(v1),
                          v2)
end

function PooledDataVecs(v1::AbstractArray,
                        v2::AbstractArray)

    ## Return two PooledDataVecs that share the same pool.

    refs1 = Array(POOLED_DATA_VEC_REF_TYPE, size(v1))
    refs2 = Array(POOLED_DATA_VEC_REF_TYPE, size(v2))
    poolref = Dict{promote_type(eltype(v1), eltype(v2)), POOLED_DATA_VEC_REF_TYPE}()
    maxref = 0

    # loop through once to fill the poolref dict
    for i = 1:length(v1)
        if !isna(v1[i])
            poolref[v1[i]] = 0
        end
    end
    for i = 1:length(v2)
        if !isna(v2[i])
            poolref[v2[i]] = 0
        end
    end

    # fill positions in poolref
    pool = sort(keys(poolref))
    i = 1
    for p in pool
        poolref[p] = i
        i += 1
    end

    # fill in newrefs
    for i = 1:length(v1)
        if isna(v1[i])
            refs1[i] = POOLED_DATA_VEC_REF_CONVERTER(0)
        else
            refs1[i] = poolref[v1[i]]
        end
    end
    for i = 1:length(v2)
        if isna(v2[i])
            refs2[i] = POOLED_DATA_VEC_REF_CONVERTER(0)
        else
            refs2[i] = poolref[v2[i]]
        end
    end

    return (PooledDataArray(refs1, pool),
            PooledDataArray(refs2, pool))
end
