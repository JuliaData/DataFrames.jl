
# A NamedArray is like a list in R or a DataFrame in Julia without the
# requirement that columns be of equal length. The main reason for its
# existence is to allow creation of a DataFrame from unequal column
# lengths like the following:
#   DataFrame(quote
#       a = 1
#       b = [1:5]
#       c = [1:10]
#   end)
type NamedArray <: Associative{Any,Any}
    data::Vector{Any}
    idx::AbstractIndex
    function NamedArray(data::Vector, idx::AbstractIndex)
        if length(idx) != length(data)
            error("index/names must be the same length as the data")
        end
        new(data, idx)
    end
end
NamedArray() = NamedArray({}, Index())

Base.length(x::NamedArray) = length(x.idx)
Base.names(x::NamedArray) = names(x.idx)

Base.getindex(x::NamedArray, c) = x[x.idx[c]]
Base.getindex(x::NamedArray, c::Integer) = x.data[c]
Base.getindex(x::NamedArray, c::Vector{Int}) = NamedArray(x.data[c], names(x)[c])

function Base.setindex!(x::NamedArray, newdata, ipos::Integer)
    if ipos > 0 && ipos <= length(x)
        x.data[ipos] = newdata
    else
        throw(ArgumentError("Can't replace a non-existent array position"))
    end
    x
end
function Base.setindex!(x::NamedArray, newdata, name)
    ipos = get(x.idx.lookup, name, 0)
    if ipos > 0
        # existing
        setindex!(x, newdata, ipos)
    else
        # new
        push!(x.idx, name)
        push!(x.data, newdata)
    end
    x
end


# Associative methods:
Base.has(x::NamedArray, key) = has(x.idx, key)
Base.get(x::NamedArray, key, default) = has(x, key) ? x[key] : default
Base.keys(x::NamedArray) = keys(x.idx)
Base.values(x::NamedArray) = x.data
# Collection methods:
Base.start(x::NamedArray) = 1
Base.done(x::NamedArray, i) = i > length(x.data)
Base.next(x::NamedArray, i) = ((x.idx.names[i], x[i]), i + 1)
Base.length(x::NamedArray) = length(x.data)
Base.isempty(x::NamedArray) = length(x.data) == 0
