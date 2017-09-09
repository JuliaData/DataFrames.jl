import Base: isidentifier, is_id_start_char, is_id_char

const RESERVED_WORDS = Set(["begin", "while", "if", "for", "try",
    "return", "break", "continue", "function", "macro", "quote", "let",
    "local", "global", "const", "abstract", "typealias", "type", "bitstype",
    "immutable", "do", "module", "baremodule", "using", "import",
    "export", "importall", "end", "else", "elseif", "catch", "finally"])

VERSION < v"0.6.0-dev.2194" && push!(RESERVED_WORDS, "ccall")
VERSION >= v"0.6.0-dev.2698" && push!(RESERVED_WORDS, "struct")

function identifier(s::AbstractString)
    s = normalize_string(s)
    if !isidentifier(s)
        s = makeidentifier(s)
    end
    Symbol(in(s, RESERVED_WORDS) ? "_"*s : s)
end

function makeidentifier(s::AbstractString)
    i = start(s)
    done(s, i) && return "x"

    res = IOBuffer(sizeof(s) + 1)

    (c, i) = next(s, i)
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

    while !done(s, i)
        (c, i) = next(s, i)
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

function make_unique(names::Vector{Symbol}; allow_duplicates=true)
    seen = Set{Symbol}()
    names = copy(names)
    dups = Int[]
    for i in 1:length(names)
        name = names[i]
        in(name, seen) ? push!(dups, i) : push!(seen, name)
    end

    if !allow_duplicates && length(dups) > 0
        d = unique(names[dups])
        msg = """Duplicate variable names: $d.
                 Pass allow_duplicates=true to make them unique using a suffix automatically."""
        throw(ArgumentError(msg))
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

#' @description
#'
#' Generate standardized names for columns of a DataFrame. The
#' first name will be :x1, the second :x2, etc.
#'
#' @field n::Integer The number of names to generate.
#'
#' @returns names::Vector{Symbol} A vector of standardized column names.
#'
#' @examples
#'
#' DataFrames.gennames(10)
function gennames(n::Integer)
    res = Array{Symbol}(n)
    for i in 1:n
        res[i] = Symbol(@sprintf "x%d" i)
    end
    return res
end


#' @description
#'
#' Count the number of null values in an array.
#'
#' @field a::AbstractArray The array whose missing values are to be counted.
#'
#' @returns count::Int The number of null values in `a`.
#'
#' @examples
#'
#' DataFrames.countnull([1, 2, 3])
function countnull(a::AbstractArray)
    res = 0
    for x in a
        res += isnull(x)
    end
    return res
end

#' @description
#'
#' Count the number of missing values in a CategoricalArray.
#'
#' @field na::CategoricalArray The CategoricalArray whose missing values
#'        are to be counted.
#'
#' @returns count::Int The number of null values in `a`.
#'
#' @examples
#'
#' DataFrames.countnull(CategoricalArray([1, 2, 3]))
function countnull(a::CategoricalArray)
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

