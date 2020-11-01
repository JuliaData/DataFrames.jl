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
    const ComposedFunction = let h = identity ∘ convert
        @assert h.f === identity
        @assert h.g === convert
        getfield(parentmodule(typeof(h)), nameof(typeof(h)))
    end
    @assert identity ∘ convert isa ComposedFunction
end

funname(c::ComposedFunction) = Symbol(funname(c.f), :_, funname(c.g))

testtype(t::Type, ct::Type, unionmissing::Bool) =
    if t === Missing
        return ct === Missing
    else
        if unionmissing
            ct === Missing && return t === Union{} || Missing <: t
            return ct <: Union{t, Missing}
        else
            return ct <: t
        end
    end
