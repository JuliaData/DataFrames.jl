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
