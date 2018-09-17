function make_unique(names::Vector{Symbol}; makeunique::Bool=false)
    seen = Set{Symbol}()
    names = copy(names)
    dups = Int[]
    for i in 1:length(names)
        name = names[i]
        in(name, seen) ? push!(dups, i) : push!(seen, name)
    end

    if length(dups) > 0
        if !makeunique
            Base.depwarn("Duplicate variable names are deprecated: pass makeunique=true to add a suffix automatically.", :make_unique)
            # TODO: uncomment the lines below after deprecation period
            # msg = """Duplicate variable names: $(u[dups]).
            #          Pass makeunique=true to make them unique using a suffix automatically."""
            # throw(ArgumentError(msg))
        end
    end

    for i in dups
        nm = names[i]
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

"""
    gennames(n::Integer)

Generate standardized names for columns of a DataFrame. The first name will be `:x1`, the
second `:x2`, etc.
"""
function gennames(n::Integer)
    res = Array{Symbol}(undef, n)
    for i in 1:n
        res[i] = Symbol(@sprintf "x%d" i)
    end
    return res
end

