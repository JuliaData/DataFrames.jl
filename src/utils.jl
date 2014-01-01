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
        if !(a[i] in b)
            push!(idx, i)
        end
    end
    a[idx]
end

## Issue: this doesn't maintain the order in a:
## setdiff(a::Vector, b::Vector) = elements(Set(a...) - Set(b...))



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
        if haskey(x, names[i])
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
            if !haskey(x, newnm)
                push!(x, newnm)
                break
            end
            k += 1
        end
        names[i] = newnm
    end
    names
end

function gennames(n::Int)
    convert(Vector{ByteString}, map(i -> "x" * string(i), 1:n))
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
    return filter(e -> (e in ancestors(s)), ancestors(t))
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
