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
function PooledDataArray{T, N}(d::Array{T, N}, m::AbstractArray{Bool, N})
    newrefs = Array(POOLED_DATA_VEC_REF_TYPE, size(d))
    #newpool = Array(T, 0)
    poolref = Dict{T, POOLED_DATA_VEC_REF_TYPE}(0) # Why isn't this a set?
    maxref = 0

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
function PooledDataArray{T, N}(d::Array{T, N},
                               pool::Vector{T},
                               m::AbstractArray{Bool, N})
    if length(pool) > typemax(POOLED_DATA_VEC_REF_TYPE)
        error("Cannot construct a PooledDataVector with such a large pool")
    end

    newrefs = Array(POOLED_DATA_VEC_REF_TYPE, size(d))
    poolref = Dict{T, POOLED_DATA_VEC_REF_TYPE}(0)
    maxref = 0

    # loop through once to fill the poolref dict
    for i = 1:length(pool)
        poolref[pool[i]] = 0
    end

    # fill positions in poolref
    newpool = sort(keys(poolref))
    i = 1
    for p in newpool
        poolref[p] = i
        i += 1
    end

    # fill in newrefs
    for i = 1:length(d)
        if m[i]
            newrefs[i] = 0
        else
            if has(poolref, d[i])
              newrefs[i] = poolref[d[i]]
            else
              error("Vector contains elements not in provided pool")
            end
        end
    end

    return PooledDataArray(newrefs, newpool)
end

# Convert a BitArray to an Array{Bool} w/ specified missingness
function PooledDataArray{N}(d::BitArray{N}, m::AbstractArray{Bool, N})
    PooledDataArray(convert(Array{Bool}, d), m)
end

# Convert a DataArray to a PooledDataArray
PooledDataArray{T}(da::DataArray{T}) = PooledDataArray(da.data, da.na)

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
function ref(::Type{PooledDataVector}, vals...)
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
##
##############################################################################

# Convert a PooledDataVector{T} to a DataVector{T}
# TODO: Make this work for Array's
function values{T}(x::PooledDataVector{T})
    n = length(x)
    res = DataArray(T, n)
    for i in 1:n
        r = x.refs[i]
        if r == 0
            res[i] = NA
        else
            res[i] = x.pool[r]
        end
    end
    return res
end
DataArray(pda::PooledDataArray) = values(pda)
values(da::DataArray) = copy(da)

function unique{T}(x::PooledDataVector{T})
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
levels{T}(pdv::PooledDataVector{T}) = unique(pdv)

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

function index_to_level{T}(x::PooledDataVector{T})
    d = Dict{POOLED_DATA_VEC_REF_TYPE, T}()
    for i in POOLED_DATA_VEC_REF_CONVERTER(1:length(x.pool))
        d[i] = x.pool[i]
    end
    return d
end

function level_to_index{T}(x::PooledDataVector{T})
    d = Dict{T, POOLED_DATA_VEC_REF_TYPE}()
    for i in POOLED_DATA_VEC_REF_CONVERTER(1:length(x.pool))
        d[x.pool[i]] = i
    end
    d
end

##############################################################################
##
## similar()
##
##############################################################################

function similar(pda::PooledDataArray, dims::Int...)
    PooledDataArray(fill(uint16(0), dims...), pda.pool)
end

function similar(pda::PooledDataArray, dims::Dims)
    PooledDataArray(fill(uint16(0), dims), pda.pool)
end

##############################################################################
##
## find()
##
##############################################################################

find(pdv::PooledDataVector{Bool}) = find(values(pdv))

##############################################################################
##
## ref()
##
##############################################################################

# dv[SingleItemIndex]
function ref(x::PooledDataVector, ind::Real)
    if x.refs[ind] == 0
        return NA
    else
        return x.pool[x.refs[ind]]
    end
end

# dv[MultiItemIndex]
function ref(x::PooledDataVector, inds::AbstractDataVector{Bool})
    inds = find(replaceNA(inds, false))
    return PooledDataArray(x.refs[inds], copy(x.pool))
end
function ref(x::PooledDataVector, inds::AbstractDataVector)
    inds = removeNA(inds)
    return PooledDataArray(x.refs[inds], copy(x.pool))
end
function ref(x::PooledDataVector, inds::Union(Vector, BitVector, Ranges))
    return PooledDataArray(x.refs[inds], copy(x.pool))
end

# TODO: Fill in other methods from DataArray

##############################################################################
##
## assign() definitions
##
##############################################################################

# x[SingleIndex] = NA
# TODO: Delete values from pool that no longer exist?
function assign(x::PooledDataVector, val::NAtype, ind::Real)
    x.refs[ind] = 0
    return NA
end

# x[SingleIndex] = Single Item
# TODO: Delete values from pool that no longer exist?
function assign(x::PooledDataVector, val::Any, ind::Real)
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
function assign(x::PooledDataVector{NAtype},
                val::NAtype,
                inds::AbstractVector{Bool})
    error("Don't use PooledDataVector{NAtype}'s")
end
function assign(x::PooledDataVector{NAtype},
                val::NAtype,
                inds::AbstractVector)
    error("Don't use PooledDataVector{NAtype}'s")
end

# x[MultiIndex] = NA
# TODO: Delete values from pool that no longer exist?
function assign(x::PooledDataVector, val::NAtype, inds::AbstractVector{Bool})
    inds = find(inds)
    x.refs[inds] = 0
    return NA
end
function assign(x::PooledDataVector, val::NAtype, inds::AbstractVector)
    x.refs[inds] = 0
    return NA
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

function show(io, x::PooledDataVector)
    print(io, "values: ")
    print(io, values(x))
    print(io, "\n")
    print(io, "levels: ")
    print(io, levels(x))
end

function repl_show{T}(io::IO, dv::PooledDataVector{T})
    n = length(dv)
    print(io, "$n-element $T PooledDataVector\n")
    if n == 0
        return
    end
    max_lines = tty_rows() - 5
    head_lim = fld(max_lines, 2)
    if mod(max_lines, 2) == 0
        tail_lim = (n - fld(max_lines, 2)) + 2
    else
        tail_lim = (n - fld(max_lines, 2)) + 1
    end
    if n > max_lines
        for i in 1:head_lim
            println(io, strcat(' ', dv[i]))
        end
        println(io, " \u22ee")
        for i in tail_lim:(n - 1)
            println(io, strcat(' ', dv[i]))
        end
        println(io, strcat(' ', dv[n]))
    else
        for i in 1:(n - 1)
            println(io, strcat(' ', dv[i]))
        end
        println(io, strcat(' ', dv[n]))
    end
    print(io, "levels: ")
    print(io, levels(dv))
end

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
## Sorting
##
## TODO: Remove
##
##############################################################################

order(pd::PooledDataArray) = groupsort_indexer(pd)[1]
sort(pd::PooledDataArray) = pd[order(pd)]

##############################################################################
##
## PooledDataVecs: EXPLANATION SHOULD GO HERE
##
##############################################################################

function PooledDataVecs{S, T}(v1::AbstractDataVector{S},
                              v2::AbstractDataVector{T})
    ## Return two PooledDataVecs that share the same pool.

    refs1 = Array(POOLED_DATA_VEC_REF_TYPE, length(v1))
    refs2 = Array(POOLED_DATA_VEC_REF_TYPE, length(v2))
    poolref = Dict{T, POOLED_DATA_VEC_REF_TYPE}(length(v1))
    maxref = 0

    # loop through once to fill the poolref dict
    for i = 1:length(v1)
        ## TODO see if we really need the NA checking here.
        ## if !isna(v1[i])
            poolref[v1[i]] = 0
        ## end
    end
    for i = 1:length(v2)
        ## if !isna(v2[i])
            poolref[v2[i]] = 0
        ## end
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
        ## if isna(v1[i])
        ##     refs1[i] = 0
        ## else
            refs1[i] = poolref[v1[i]]
        ## end
    end
    for i = 1:length(v2)
        ## if isna(v2[i])
        ##     refs2[i] = 0
        ## else
            refs2[i] = poolref[v2[i]]
        ## end
    end
    (PooledDataArray(refs1, pool),
     PooledDataArray(refs2, pool))
end
