
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

length(x::NamedArray) = length(x.idx)
names(x::NamedArray) = names(x.idx)

getindex(x::NamedArray, c) = x[x.idx[c]]
getindex(x::NamedArray, c::Integer) = x.data[c]
getindex(x::NamedArray, c::Vector{Int}) = NamedArray(x.data[c], names(x)[c])

function setindex!(x::NamedArray, newdata, ipos::Integer)
    if ipos > 0 && ipos <= length(x)
        x.data[ipos] = newdata
    else
        throw(ArgumentError("Can't replace a non-existent array position"))
    end
    x
end
function setindex!(x::NamedArray, newdata, name)
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
has(x::NamedArray, key) = has(x.idx, key)
get(x::NamedArray, key, default) = has(x, key) ? x[key] : default
keys(x::NamedArray) = keys(x.idx)
values(x::NamedArray) = x.data
# Collection methods:
start(x::NamedArray) = 1
done(x::NamedArray, i) = i > length(x.data)
next(x::NamedArray, i) = ((x.idx.names[i], x[i]), i + 1)
length(x::NamedArray) = length(x.data)
isempty(x::NamedArray) = length(x.data) == 0
