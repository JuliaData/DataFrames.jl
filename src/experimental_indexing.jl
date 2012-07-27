# This is an experiment in adding indexing to vectors. The idea is
# that if a DataFrame column is indexed, the following will have fast
# lookups:
#     df[:(2 .< col1 .< 7)]
#     df[:( datecol .>= "2011-01-01" ), "col3"]
#     df[:( col .== "red" )]
#
# Keeping indexing with columns has advantages:
#     - Column ordering is less likely to be messed up inadvertantly.
#     - Multiple columns can be indexed; there's no main key grouping.
#     - More flexible logic combinations are possible.
#     - You can mix and match keyed and non-keyed comparisons.
#     - The DataFrame structure and indexing are not disturbed.
#     - It's probably less coding and maintenance.
# The disadvantages are:
#     - It's yet another vector type.
#     - It's somewhat slower than having one overall set of DataFrame
#       keys in a sorted DataFrame. It should still be pretty fast (no
#       speed checks, yet).
#     - You can't do data.table/pandas shortcuts like df["A"] for
#       df[:( keycol .== "A" )]. (But, df["A"] is less descriptive if
#       you don't know what df's keys are.)
# 
# Right now, you can't do:
#     IndexedVec(DataVec[1,2,3,NA])
# because DataVecs are not AbstractVectors. I hope we move to that.
#
# Note: some of the set operations (like _set_and) might be pretty slow.
# These might negate the advantages of indexing for complicated logic.


type IndexedVec{T} <: AbstractVector{T}
    x::AbstractVector{T}
    idx::Vector{Int}
end
IndexedVec(x::AbstractVector) = IndexedVec(x, order(x))
IndexedVec(x::Number) = x   # don't make indexes here
IndexedVec(x::String) = x

ref{T,I<:Integer}(v::IndexedVec{T},i::AbstractVector{I}) = IndexedVec(v.x[i])
ref{T}(v::IndexedVec{T},i::Integer) = v.x[i]
ref{T}(v::IndexedVec{T}, i) = IndexedVec(v.x[i])
assign(v::IndexedVec, i, val) = IndexedVec(assign(v.x, i, val))

order(x::IndexedVec) = x.idx
sort(x::IndexedVec) = x.x[x.idx]   # Return regular array?
sort(x::IndexedVec) = IndexedVec(x.x[x.idx], 1:length(x.x))   # or keep this an IndexedVec, like this?

type Indexer
    idx::Indices
    len::Int
end
type NegIndexer
    idx::Indices
    len::Int
end
!(x::Indexer) = NegIndexer(x.idx, x.len)
!(x::NegIndexer) = Indexer(x.idx, x.len)
|(x1::Indexer, x2::Indexer) = Indexer(_set_union(x1.idx, x2.idx), max(x1.len, x2.len))
|(x1::Indexer, x2::Indices) = Indexer(_set_union(x1.idx, x2), x1.len)
|(x1::Indices, x2::Indexer) = Indexer(_set_union(x1, x2.idx), x2.len)
|(x1::Indexer, x2::NegIndexer) = Indexer(_set_union(x1.idx, _setdiff([1:x2.len], x2.idx)), max(x1.len, x2.len))
|(x1::Indices, x2::NegIndexer) = Indexer(_set_union(x1, _setdiff([1:x2.len], x2.idx)), x2.len)
|(x1::NegIndexer, x2::NegIndexer) = NegIndexer(_set_union(x1.idx, x2.idx), max(x1.len, x2.len))
|(x1::NegIndexer, x2::Indexer) = Indexer(_set_union(_setdiff([1:x1.len], x1.idx), x2.idx), max(x1.len, x2.len)) # wrong
function |(x1::Indexer, x2::AbstractVector{Bool})
    x2 = copy(x2)
    x2[x1.idx] = true
    x2
end
(&)(x1::Indexer, x2::Indexer) = Indexer(_set_and(x1.idx, x2.idx), max(x1.len, x2.len))
(&)(x1::Indexer, x2::Indices) = Indexer(_set_and(x1.idx, x2), x1.len)
(&)(x1::Indices, x2::Indexer) = Indexer(_set_and(x1, x2.idx), x2.len)
(&)(x1::Indexer, x2::NegIndexer) = Indexer(_set_and(x1.idx, _setdiff([1:x2.len], x2.idx)), max(x1.len, x2.len))
(&)(x1::Indices, x2::NegIndexer) = Indexer(_set_and(x1, _setdiff([1:x2.len], x2.idx)), x2.len)
(&)(x1::NegIndexer, x2::NegIndexer) = NegIndexer(_set_and(x1.idx, x2.idx), max(x1.len, x2.len))
(&)(x1::NegIndexer, x2::Indexer) = Indexer(_set_and(_setdiff([1:x1.len], x1.idx), x2.idx), max(x1.len, x2.len)) # wrong

ref(x::AbstractVector, i::Indexer) = x[i.idx]
ref(x::AbstractVector, i::NegIndexer) = x[_setdiff([1:end], i.idx)]

# element-wise (in)equality operators
# these may need range checks
# Should these be sorted? Could be a counting sort.
.=={T}(a::IndexedVec{T}, v::T) = Indexer(a.idx[search_sorted_first(a.x, v, a.idx) : search_sorted_last(a.x, v, a.idx)], length(a))
.>={T}(a::IndexedVec{T}, v::T) = Indexer(a.idx[search_sorted_first(a.x, v, a.idx) : end], length(a))
.<={T}(a::IndexedVec{T}, v::T) = Indexer(a.idx[1 : search_sorted_last(a.x, v, a.idx)], length(a))
.>{T}(a::IndexedVec{T}, v::T) = Indexer(a.idx[search_sorted_first_gt(a.x, v, a.idx) : end], length(a))
.<{T}(a::IndexedVec{T}, v::T) = Indexer(a.idx[1 : search_sorted_last_lt(a.x, v, a.idx)], length(a))
.=={T}(v::T, a::IndexedVec{T}) = Indexer(a.idx[search_sorted_first(a.x, v, a.idx) : search_sorted_last(a.x, v, a.idx)], length(a))
.>={T}(v::T, a::IndexedVec{T}) = Indexer(a.idx[search_sorted_first(a.x, v, a.idx) : end], length(a))
.<={T}(v::T, a::IndexedVec{T}) = Indexer(a.idx[1 : search_sorted_last(a.x, v, a.idx)], length(a))
.>{T}(v::T, a::IndexedVec{T}) = !(v .<= a)
.<{T}(v::T, a::IndexedVec{T}) = !(v .>= a) 

function search_sorted_first_gt{I<:Integer}(a::AbstractVector, x, idx::AbstractVector{I})
    res = search_sorted_last(a, x, idx)
    if res == 0 return 1 end
    if res == length(a) && a[idx[res]] != x return(length(a)+1) end 
    a[idx[res]] == x ? res + 1 : res
end
function search_sorted_last_lt{I<:Integer}(a::AbstractVector, x, idx::AbstractVector{I})
    res = search_sorted_first(a, x, idx)
    if res > length(a) return length(a) end
    if res == 1 && a[idx[res]] != x return(0) end 
    a[idx[res]] == x ? res - 1 : res
end

eps(i::Int64) = 1
eps(i::Int32) = int32(1)

function in{T}(a::IndexedVec{T}, y::Vector{T})
    res = similar({}, length(y))
    for i in 1:length(y)
        res[i] = (a .== y[i]).idx
    end
    vcat(res...)
end

between{T}(a::IndexedVec{T}, v1::T, v2::T, ) = Indexer(a.idx[search_sorted_first(a.x, v1, a.idx) : search_sorted_last(a.x, v2, a.idx)], length(a))

size(a::IndexedVec) = size(a.x)
length(a::IndexedVec) = length(a.x)
ndims(a::IndexedVec) = 1
numel(a::IndexedVec) = length(a.x)
eltype{T}(a::IndexedVec{T}) = T

print(io, a::IndexedVec) = print(io, a.x)
show(io, a::IndexedVec) = show(io, a.x)
repl_show(io, a::IndexedVec) = repl_show(io, a.x)

function search_sorted_last{I<:Integer}(a::AbstractVector, x, idx::AbstractVector{I})
    ## Index of the last value of vector a that is less than or equal to x.
    ## Returns 0 if x is less than all values of a.
    ## idx is an indexing vector equal in length to a that sorts a 
    @assert length(a) == length(idx)
    lo = 0
    hi = length(a) + 1
    while lo < hi-1
        i = (lo+hi)>>>1
        if isless(x,a[idx[i]])
            hi = i
        else
            lo = i
        end
    end
    lo
end

function search_sorted_first{I<:Integer}(a::AbstractVector, x, idx::AbstractVector{I})
    ## Index of the first value of vector a that is greater than or equal to x.
    ## Returns length(a) + 1 if x is greater than all values in a.
    ## idx is an indexing vector equal in length to a that sorts a
    @assert length(a) == length(idx)
    lo = 0
    hi = length(a) + 1
    while lo < hi-1
        i = (lo+hi)>>>1
        if isless(a[idx[i]],x)
            lo = i
        else
            hi = i
        end
    end
    hi
end

# Should the sort's be included?
_set_union(x1, x2) = sort(unique([x1, x2]))
_set_diff(x1, x2) = sort(_setdiff(x1, x2))
function _set_and(x1::AbstractVector, x2::AbstractVector)
    if length(x1) < length(x2)
        res = x1
        base = x2
    else
        res = x2
        base = x1
    end
    N = length(res)
    d = Dict(N)
    for i in 1:length(base)
        d[base[i]] = 1
    end
    idx = fill(false, N)
    for i in 1:length(res)
        if has(d, res[i])
            idx[i] = true
        end
    end
    sort(res[idx])
end



# Examples
srand(1)
a = randi(5,20)
ia = IndexedVec(a)
ia .== 4
v = [1:20]
v[ia .== 4]

(ia .== 4) | (ia .== 5)

v[(ia .== 4) | (ia .== 5)]

v[(ia .>= 4) | (ia .== 5)] | println
ia[(ia .>= 4) | (ia .== 5)].x | println
v[(ia .>= 4) & (ia .== 5)] | println
ia[(ia .>= 4) & (ia .== 5)].x | println

# the following was needed for show(df)
maxShowLength(v::IndexedVec) = length(v) > 0 ? max([length(_string(x)) for x = v.x]) : 0

df = DataFrame({IndexedVec(vcat(fill([1:5],4)...)), IndexedVec(vcat(fill(letters[1:10],2)...))})

df[:(x2 .== "a")]
df[:( (x2 .== "a") | (x1 .== 2) )] 
df[:( ("b" .<= x2 .<= "c") | (x1 .== 5) )]
df[:( (x1 .== 1) & (x2 .== "a") )]
df[:( in(x2, ["c","e"]) )]
