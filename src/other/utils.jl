import Base: isidentifier, is_id_start_char, is_id_char

const RESERVED_WORDS = Set(["begin", "while", "if", "for", "try",
    "return", "break", "continue", "function", "macro", "quote", "let",
    "local", "global", "const", "abstract", "typealias", "type", "bitstype",
    "immutable", "ccall", "do", "module", "baremodule", "using", "import",
    "export", "importall", "end", "else", "elseif", "catch", "finally"])
if VERSION >= v"0.4.0-dev+757"
    push!(RESERVED_WORDS, "stagedfunction")
end

function identifier(s::AbstractString)
    s = normalize_string(s)
    if !isidentifier(s)
        s = makeidentifier(s)
    end
    @compat(Symbol(in(s, RESERVED_WORDS) ? "_"*s : s))
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

    return takebuf_string(res)
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
    res = Array(Symbol, n)
    for i in 1:n
        res[i] = Symbol(@sprintf "x%d" i)
    end
    return res
end

#' @description
#'
#' Count the number of missing values in an Array.
#'
#' NOTE: This function always returns 0.
#'
#' @field a::Array The Array whose missing values are to be counted.
#'
#' @returns count::Int The number of missing values in `a`.
#'
#' @examples
#'
#' DataFrames.countna([1, 2, 3])
countna(a::Array) = 0

#' @description
#'
#' Count the number of missing values in a DataArray.
#'
#' @field da::DataArray The DataArray whose missing values are to be counted.
#'
#' @returns count::Int The number of missing values in `a`.
#'
#' @examples
#'
#' DataFrames.countna(@data([1, 2, 3]))
countna(da::DataArray) = sum(da.na)

#' @description
#'
#' Count the number of missing values in a PooledDataArray.
#'
#' @field pda::PooledDataArray The PooledDataArray whose missing values
#'        are to be counted.
#'
#' @returns count::Int The number of missing values in `a`.
#'
#' @examples
#'
#' DataFrames.countna(@pdata([1, 2, 3]))
function countna(da::PooledDataArray)
    res = 0
    for i in 1:length(da)
        res += da.refs[i] == 0
    end
    return res
end

function _setdiff{T}(a::AbstractVector{T}, b::AbstractVector{T})
    diff = T[]
    for val in a
        if !(val in b)
            push!(diff, val)
        end
    end
    diff
end
# because unions and parametric types don't compose, yet
function _setdiff{T}(a::AbstractVector{T}, b::T)
    diff = T[]
    for val in a
        if !(val in b)
            push!(diff, val)
        end
    end
    diff
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

# Gets the name of a function. Used in groupedataframe/grouping.jl
function _fnames{T<:Function}(fs::Vector{T})
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
