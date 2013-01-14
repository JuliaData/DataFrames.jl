##############################################################################
##
## Default values for unspecified objects
##
## Sometimes needed when dealing with NA's for which some value must exist in
## the underlying data vector
##
##############################################################################

baseval{T <: String}(s::Type{T}) = ""
baseval(x::Any) = zero(x)

# slow, but maintains order and seems to work:
function _setdiff(a::Vector, b::Vector)
    idx = Int[]
    for i in 1:length(a)
        if !contains(b, a[i])
            push!(idx, i)
        end
    end
    a[idx]
end

## Issue: this doesn't maintain the order in a:
## setdiff(a::Vector, b::Vector) = elements(Set(a...) - Set(b...))

# MAYBE try to get in base?
function fill(x::Vector, lengths::Vector{Int})
    if length(x) != length(lengths)
        error("vector lengths must match")
    end
    res = similar(x, sum(lengths))
    i = 1
    for idx in 1:length(x)
        tmp = x[idx]
        for kdx in 1:lengths[idx]
            res[i] = tmp
            i += 1
        end
    end
    res
end

function _uniqueofsorted(x::Vector)
    idx = fill(true, length(x))
    lastx = x[1]
    for i = 2:length(x)
        if lastx == x[i]
            idx[i] = false
        else
            lastx = x[i]
        end
    end
    x[idx]
end

function make_unique{S<:ByteString}(names::Vector{S})
    x = Index()
    names = copy(names)
    dups = Int[]
    for i in 1:length(names)
        if has(x, names[i])
            push!(dups, i)
        else
            push!(x, names[i])
        end
    end
    for i in dups
        nm = names[i]
        newnm = nm
        k = 1
        while true
            newnm = "$(nm)_$k"
            if !has(x, newnm)
                push!(x, newnm)
                break
            end
            k += 1
        end
        names[i] = newnm
    end
    names
end

# TODO: move to set.jl? call nointer or nodupes?
# reasonably fast approach: foreach argument, iterate over
# the (presumed) set, checking for duplicates, adding to a hash table as we go
function nointer(ss...)
    d = Dict{Any,Int}(0)
    for s in ss
        for item in s
            ct = get(d, item, 0)
            if ct == 0 # we're good, add it
                d[item] = 1
            else
                return false
            end
        end
    end
    return true
end

function concat{T1,T2}(v1::Vector{T1}, v2::Vector{T2})
    # concatenate vectors, converting to type Any if needed.
    if T1 == T2 && T1 != Any
        [v1, v2]
    else
        res = Array(Any, length(v1) + length(v2))
        res[1:length(v1)] = v1
        res[length(v1)+1 : length(v1)+length(v2)] = v2
        res
    end
end

function _same_set(a, b)
    # there are definitely MUCH faster ways of doing this
    length(a) == length(b) && all(sort(a) == sort(b))
end

function generate_column_names(n::Int)
    convert(Vector{ByteString}, map(i -> "x" * string(i), 1:n))
end

function repeat(dv::AbstractVector, n::Int)
    m = length(dv)
    if rem(n, m) != 0
        error("New size must be a multiple of input vector's length")
    end
    new_dv = similar(dv, n)
    for i in 0:(fld(n, m) - 1)
        for j in 1:m
            new_j = i * m + j
            new_dv[new_j] = dv[j]
        end
    end
    return new_dv
end

function ancestors(t::Type)
    a = {t}
    while t != Any
        t = super(t)
        push!(a, t)
    end
    return a
end

function common_ancestors(s::Type, t::Type)
    return filter(e -> contains(ancestors(s), e), ancestors(t))
end

earliest_common_ancestor(s::Type, t::Type) = first(common_ancestors(s, t))

# Need to do type inference
# Like earliest_common_ancestor, but ignores NA
function _dv_most_generic_type(vals)
    # iterate over vals tuple to find the most generic non-NA type
    toptype = None
    for i = 1:length(vals)
        if !isna(vals[i])
            toptype = promote_type(toptype, typeof(vals[i]))
        end
    end
    if !method_exists(baseval, (toptype, ))
        error("No baseval exists for type: $(toptype)")
    end
    return toptype
end
