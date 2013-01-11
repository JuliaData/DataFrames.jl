# Super-hacked out constructor: DataVector[1, 2, NA]
# Need to do type inference
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
function ref(::Type{DataVector}, vals...)
    # Get the most generic non-NA type
    toptype = _dv_most_generic_type(vals)

    # Allocate an empty DataVector
    lenvals = length(vals)
    res = DataArray(Array(toptype, lenvals), BitArray(lenvals))

    # Copy from vals into data and mask
    for i = 1:lenvals
        if isna(vals[i])
            res.data[i] = baseval(toptype)
            res.na[i] = true
        else
            res.data[i] = vals[i]
            res.na[i] = false
        end
    end

    return res
end

##############################################################################
##
## isna()
##
##############################################################################

isna(x::Any) = false

##############################################################################
##
## find()
##
##############################################################################

function find(dv::DataVector{Bool})
    n = length(dv)
    res = Array(Int, n)
    bound = 0
    for i in 1:length(dv)
        if !dv.na[i] && dv.data[i]
            bound += 1
            res[bound] = i
        end
    end
    return res[1:bound]
end

##############################################################################
##
## String representations and printing
##
## TODO: Inherit these from AbstractArray after implementing DataArray
##
##############################################################################

function string(x::AbstractDataVector)
    tmp = join(x, ", ")
    return "[$tmp]"
end

show(io, x::AbstractDataVector) = Base.show_comma_array(io, x, '[', ']')

function repl_show{T}(io::IO, dv::DataVector{T})
    n = length(dv)
    print(io, "$n-element $T DataArray\n")
    if n == 0
        return
    end
    max_lines = tty_rows() - 4
    head_lim = fld(max_lines, 2)
    if mod(max_lines, 2) == 0
        tail_lim = (n - fld(max_lines, 2)) + 2
    else
        tail_lim = (n - fld(max_lines, 2)) + 1
    end
    if n > max_lines
        for i in 1:head_lim
            println(io, strcat(' ', dv[i]))
        end
        println(io, " \u22ee")
        for i in tail_lim:(n - 1)
            println(io, strcat(' ', dv[i]))
        end
        print(io, strcat(' ', dv[n]))
    else
        for i in 1:(n - 1)
            println(io, strcat(' ', dv[i]))
        end
        print(io, strcat(' ', dv[n]))
    end
end

head{T}(dv::DataVector{T}) = repl_show(dv[1:min(6, length(dv))])

tail{T}(dv::DataVector{T}) = repl_show(dv[max(length(dv) - 6, 1):length(dv)])

##############################################################################
##
## Container operations
##
##############################################################################

# TODO: Fill in definitions for PooledDataVector's
# TODO: Macroize these definitions

function push{T}(dv::DataVector{T}, v::NAtype)
    push!(dv.data, baseval(T))
    push!(dv.na, true)
    return v
end

function push{S, T}(dv::DataVector{S}, v::T)
    push!(dv.data, v)
    push!(dv.na, false)
    return v
end

function pop{T}(dv::DataVector{T})
    d, m = pop!(dv.data), pop!(dv.na)
    if m
        return NA
    else
        return d
    end
end

function enqueue{T}(dv::DataVector{T}, v::NAtype)
    enqueue!(dv.data, baseval(T))
    enqueue!(dv.na, true)
    return v
end

function enqueue{S, T}(dv::DataVector{S}, v::T)
    enqueue!(dv.data, v)
    enqueue!(dv.na, false)
    return v
end

function shift{T}(dv::DataVector{T})
    d, m = shift(dv.data), shift(dv.na)
    if m
        return NA
    else
        return d
    end
end

function map{T}(f::Function, dv::DataVector{T})
    n = length(dv)
    res = DataArray(Any, n)
    for i in 1:n
        res[i] = f(dv[i])
    end
    return res
end

##############################################################################
##
## table()
##
##############################################################################

function table{T}(d::AbstractDataVector{T})
    counts = Dict{Union(T, NAtype), Int}(0)
    for i = 1:length(d)
        if has(counts, d[i])
            counts[d[i]] += 1
        else
            counts[d[i]] = 1
        end
    end
    return counts
end

##############################################################################
##
## paste()
##
##############################################################################

const letters = convert(Vector{ASCIIString}, split("abcdefghijklmnopqrstuvwxyz", ""))
const LETTERS = convert(Vector{ASCIIString}, split("ABCDEFGHIJKLMNOPQRSTUVWXYZ", ""))

# Like string(s), but preserves Vector{String} and converts
# Vector{Any} to Vector{String}.
_vstring{T <: String}(s::T) = s
_vstring{T <: String}(s::Vector{T}) = s
_vstring(s::Vector) = map(_vstring, s)
_vstring(s::Any) = string(s)

function paste{T<:String}(s::Vector{T}...)
    sa = {s...}
    N = max(length, sa)
    res = fill("", N)
    for i in 1:length(sa)
        Ni = length(sa[i])
        k = 1
        for j = 1:N
            res[j] = strcat(res[j], sa[i][k])
            if k == Ni   # This recycles array elements.
                k = 1
            else
                k += 1
            end
        end
    end
    res
end
# The following converts all arguments to Vector{<:String} before
# calling paste.
function paste(s...)
    converted = map(vcat * _vstring, {s...})
    paste(converted...)
end

##############################################################################
##
## cut()
##
##############################################################################

function cut{S, T}(x::Vector{S}, breaks::Vector{T})
    if !issorted(breaks)
        sort!(breaks)
    end
    min_x, max_x = min(x), max(x)
    if breaks[1] > min_x
        unshift!(breaks, min_x)
    end
    if breaks[end] < max_x
        push!(breaks, max_x)
    end
    refs = fill(POOLED_DATA_VEC_REF_CONVERTER(0), length(x))
    for i in 1:length(x)
        if x[i] == min_x
            refs[i] = 1
        else
            refs[i] = search_sorted(breaks, x[i]) - 1
        end
    end
    n = length(breaks)
    from = map(x -> sprint(showcompact, x), breaks[1:(n - 1)])
    to = map(x -> sprint(showcompact, x), breaks[2:n])
    pool = Array(ASCIIString, n - 1)
    if breaks[1] == min_x
        pool[1] = strcat("[", from[1], ",", to[1], "]")
    else
        pool[1] = strcat("(", from[1], ",", to[1], "]")
    end
    for i in 2:(n - 1)
        pool[i] = strcat("(", from[i], ",", to[i], "]")
    end
    PooledDataArray(refs, pool)
end
cut(x::Vector, ngroups::Int) = cut(x, quantile(x, [1 : ngroups - 1] / ngroups))
