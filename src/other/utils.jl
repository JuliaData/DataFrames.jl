"""
    AsTable(cols)

A type used for selection operations to signal that the columns selected by the
wrapped selector should be passed as a `NamedTuple` to the function.
"""
struct AsTable
    cols
end

Base.broadcastable(x::AsTable) = Ref(x)

function make_unique!(names::Vector{Symbol}, src::AbstractVector{Symbol};
                      makeunique::Bool=false)
    if length(names) != length(src)
        throw(DimensionMismatch("Length of src doesn't match length of names."))
    end
    seen = Set{Symbol}()
    dups = Int[]
    for i in 1:length(names)
        name = src[i]
        if in(name, seen)
            push!(dups, i)
        else
            names[i] = src[i]
            push!(seen, name)
        end
    end

    if length(dups) > 0
        if !makeunique
            dupstr = join(string.(':', unique(src[dups])), ", ", " and ")
            msg = "Duplicate variable names: $dupstr. Pass makeunique=true " *
                  "to make them unique using a suffix automatically."
            throw(ArgumentError(msg))
        end
    end

    for i in dups
        nm = src[i]
        k = 1
        while true
            newnm = Symbol("$(nm)_$k")
            if !in(newnm, seen)
                names[i] = newnm
                push!(seen, newnm)
                break
            end
            k += 1
        end
    end

    return names
end

function make_unique(names::AbstractVector{Symbol}; makeunique::Bool=false)
    make_unique!(similar(names), names, makeunique=makeunique)
end

"""
    gennames(n::Integer)

Generate standardized names for columns of a DataFrame.
The first name will be `:x1`, the second `:x2`, etc.
"""
function gennames(n::Integer)
    res = Vector{Symbol}(undef, n)
    for i in 1:n
        res[i] = Symbol(@sprintf "x%d" i)
    end
    return res
end

function funname(f)
    n = nameof(f)
    String(n)[1] == '#' ? :function : n
end

if isdefined(Base, :ComposedFunction) # Julia >= 1.6.0-DEV.85
    using Base: ComposedFunction
else
    using Compat: ComposedFunction
end

funname(c::ComposedFunction) = Symbol(funname(c.outer), :_, funname(c.inner))

macro threadsif(cond, ex)
    quote
        if $(esc(cond))
            @threads $ex
        else
            $(esc(ex))
        end
    end
end

if VERSION >= v"1.4"
    const _partition = Iterators.partition
else
    function _partition(xs, n)
        @assert firstindex(xs) == 1
        m = cld(length(xs), n)
        return (view(xs, i*n+1:min((i+1)*n, length(xs))) for i in 0:m-1)
    end
end

function tforeach(f, xs::AbstractArray; basesize::Integer)
    nt = min(NTHREADS[], Threads.nthreads())
    if nt > 1 && length(xs) > basesize
        # Ensure we don't create more than 10 times more tasks than available threads
        basesize′ = min(basesize, length(xs) ÷ nt * 10)
        @sync for p in _partition(xs, basesize′)
            Threads.@spawn begin
                for i in p
                    f(@inbounds xs[i])
                end
            end
        end
    else
        for i in eachindex(xs)
            f(@inbounds xs[i])
        end
    end
    return
end
