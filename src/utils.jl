# slow, but maintains order and seems to work:
function _setdiff(a::Vector, b::Vector)
    idx = Int[]
    for i in 1:length(a)
        if !contains(b, a[i])
            push(idx, i)
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


function unique(x::Vector)
    idx = fill(true, length(x))
    d = Dict()
    d[x[1]] = true
    for i = 2:length(x)
        if has(d, x[i])
            idx[i] = false
        else
            d[x[i]] = true
        end
    end
    x[idx]
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
            push(dups, i)
        else
            push(x, names[i])
        end
    end
    for i in dups
        nm = names[i]
        newnm = nm
        k = 1
        while true
            newnm = "$(nm)_$k"
            if !has(x, newnm)
                push(x, newnm)
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

# const INTREGEX = r"^(-)?\d+$"
# function int_able{T <: String}(s::T)
#   ismatch(INTREGEX, s)
# end

# const FLOATREGEX = r"^(-)?\d+(\.\d+(e(-?)\d+)?)?$"
# function float_able{T <: String}(s::T)
#   ismatch(FLOATREGEX, s)
# end

# const INT64TYPE = 1
# const FLOAT64TYPE = 2
# const UTF8TYPE = 3

function tightest_type{S <: String}(s::S, t::Int)
  if t == UTF8TYPE
    return UTF8TYPE
  elseif t == FLOAT64TYPE
    if float_able(s)
      return FLOAT64TYPE
    else
      return UTF8TYPE
    end
  elseif t == INT64TYPE
    if int_able(s)
      return INT64TYPE
    elseif float_able(s)
      return FLOAT64TYPE
    else
      return UTF8TYPE
    end
  end
end

function int_able{T <: String}(s::T)
    d = s.data
    if length(d) == 0
        return false
    end
    p = 1
    if d[p] == '-'
        p += 1
        if length(d) == 1
            return false
        end
    end
    while p <= length(d)
        if !contains("1234567890", d[p])
            return false
        end
        p += 1
    end        
    return true
end

function float_able{T <: String}(s::T)
    d = s.data
    if length(d) == 0
        return false
    end
    p = 1
    if d[p] == '-'
        p += 1
        if length(d) < p
            return false
        end
    end
    dot = false
    exp = false
    while p <= length(d)
        if !contains("1234567890", d[p])
            if d[p] == 'e'
                if p == 1 || d[p-1] == '-'
                    return false
                end
                if exp
                    return false
                end
                exp = true
                p += 1
                if length(d) < p
                    return false
                end
                if d[p] == '-'
                    p += 1
                    if length(d) < p
                        return false
                    end
                end
            elseif d[p] == '.' && exp == false
                if dot
                    return false
                end
                dot = true
            else
                return false
            end
        end
        p += 1
    end
    return true
end
