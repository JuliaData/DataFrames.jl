import Base: isidentifier, is_id_start_char, is_id_char

const RESERVED_WORDS = Set(["local", "global", "export", "let",
    "for", "struct", "while", "const", "continue", "import",
    "function", "if", "else", "try", "begin", "break", "catch",
    "return", "using", "baremodule", "macro", "finally",
    "module", "elseif", "end", "quote", "do"])

function identifier(s::AbstractString)
    s = Unicode.normalize(s)
    if !isidentifier(s)
        s = makeidentifier(s)
    end
    Symbol(in(s, RESERVED_WORDS) ? "_"*s : s)
end

function makeidentifier(s::AbstractString)
    (iresult = iterate(s)) === nothing && return "x"

    res = IOBuffer(zeros(UInt8, sizeof(s)+1), write=true)

    (c, i) = iresult
    under = if is_id_start_char(c)
        write(res, c)
        c == '_'
    elseif is_id_char(c)
        write(res, 'x', c)
        false
    else
        write(res, '_')
        true
    end

    while (iresult = iterate(s, i)) !== nothing
        (c, i) = iresult
        if c != '_' && is_id_char(c)
            write(res, c)
            under = false
        elseif !under
            write(res, '_')
            under = true
        end
    end

    return String(take!(res))
end

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


"""
    countmissing(a::AbstractArray)

Count the number of `missing` values in an array.
"""
function countmissing(a::AbstractArray)
    res = 0
    for x in a
        res += ismissing(x)
    end
    return res
end

function countmissing(a::CategoricalArray)
    res = 0
    for x in a.refs
        res += x == 0
    end
    return res
end

# Gets the name of a function. Used in groupeDataFrame/grouping.jl
function _fnames(fs::Vector{T}) where T<:Function
    λcounter = 0
    names = map(fs) do f
        name = string(f)
        if name == "(anonymous function)" # Anonymous functions with Julia < 0.5
            λcounter += 1
            name = "λ$(λcounter)"
        end
        name
    end
    names
end
