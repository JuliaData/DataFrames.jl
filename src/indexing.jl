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


type IndexedVec{T} <: AbstractVector{T}
    x::AbstractVector{T}
    idx::Vector{Int}
end
IndexedVec(x::AbstractVector) = IndexedVec(x, order(x))

ref{T,I<:Integer}(v::IndexedVec{T},i::AbstractVector{I}) = IndexedVec(v.x[i])
ref{T}(v::IndexedVec{T},i::Integer) = v.x[i]
ref{T}(v::IndexedVec{T}, i) = IndexedVec(v.x[i])
assign(v::IndexedVec, i, val) = IndexedVec(assign(v.x, i, val))

order(x::IndexedVec) = x.idx
sort(x::IndexedVec) = x.x[x.idx]   # Return regular array?
sort(x::IndexedVec) = IndexedVec(x.x[x.idx], 1:length(x.x))   # or keep this an IndexedVec, like this?

type Indexer
    r::Vector{Range1}
    iv::IndexedVec
end

function intersect(v1::Vector{Range1}, v2::Vector{Range1})
    # Assumes that the Range1's in each vector are sorted and don't overlap.
    # (Actually, it doesn't, but it should to get more speed.)
    res = Range1[]
    # This does more work than it needs to.
    for r1 in v1
        for r2 in v2
            isoverlap = !((r1[1] > r2[end]) | (r2[1] > r1[end]))
            if isoverlap
                push(res, max(r1[1], r2[1]):min(r1[end], r2[end]))
            end
        end
    end
    res
end

function union(v1::Vector{Range1}, v2::Vector{Range1})
    # Assumes that the Range1's in each vector are sorted and don't overlap.
    # (Actually, it doesn't, but it should to get more speed.)
    # TODO Check for zero length.
    compare(v1, v2) = (v1[end][end] > v2[end][end] ? v2 : v1,
                       v1[end][end] > v2[end][end] ? v1 : v2)
    res = Range1[]
    v1 = copy(v1) # Destructively operate on these
    v2 = copy(v2)
    while length(v1) > 0 && length(v2) > 0
        # right part, working right to left
        (left, right) = compare(v1, v2)
        while length(right) > 0 && right[end][1] > left[end][end]
            push(res, pop(right))
        end
        if length(right) == 0 break; end
        # overlap
        r_end = max(right[end][end], left[end][end])  
        overlap = false
        while length(left) > 0 && length(right) > 0 && right[end][end] > left[end][1]
            r_start = min(right[end][1], left[end][1])
            overlap = true
            (left, right) = compare(v1, v2)
            if right[end][1] >= r_start
                pop(right)
            end
        end
        if overlap
            if length(right) == 0 && length(left) > 0 && r_start <= left[end][1]
                r_start = left[end][1]
                pop(left)
            end
            push(res, r_start : r_end)
        end
    end
    # Rest of v1 or v2 (no overlaps here)
    while length(v1) > 0
        push(res, pop(v1))
    end
    while length(v2) > 0
        push(res, pop(v2))
    end
    reverse(res)
end

function (!)(x::Indexer)
    res = Range1[1 : x.r[1][1] - 1]
    for i in 1:length(x.r) - 1
        push(res, x.r[i][end] + 1 : x.r[i + 1][1] - 1)
    end
    push(res, x.r[end][end] + 1 : length(x.iv))
    Indexer(res, x.iv)
end

function (&)(x1::Indexer, x2::Indexer)
    if x1.iv == x2.iv
        Indexer(intersect(x1.r, x2.r), x1.iv)
    else
        bool(x1) & bool(x2)
    end
end
(&)(x1::Vector{Bool}, x2::Indexer) = x1 & bool(x2)
(&)(x1::Indexer, x2::Vector{Bool}) = x2 & bool(x1)

function (|)(x1::Indexer, x2::Indexer)
    if x1.iv == x2.iv
        Indexer(union(x1.r, x2.r), x1.iv)
    else
        bool(x1) | bool(x2)
    end
end
(|)(x1::Vector{Bool}, x2::Indexer) = x1 | bool(x2)
(|)(x1::Indexer, x2::Vector{Bool}) = x2 | bool(x1)

function bool(ix::Indexer)
    res = fill(false, length(ix.iv.idx))
    for i in ix.iv.idx[[ix.r...]]
        res[i] = true
    end
    res
end

ref(x::IndexedVec, i::Indexer) = x[i.iv.idx[[i.r...]]]
ref(x::AbstractVector, i::Indexer) = x[i.iv.idx[[i.r...]]]
ref(x::AbstractDataVec, i::Indexer) = x[i.iv.idx[[i.r...]]]

# element-wise (in)equality operators
# these may need range checks
# Should these be sorted? Could be a counting sort.
.=={T}(a::IndexedVec{T}, v::T) = Indexer(Range1[search_sorted_first(a.x, v, a.idx) : search_sorted_last(a.x, v, a.idx)], a)
.=={T}(v::T, a::IndexedVec{T}) = Indexer(Range1[search_sorted_first(a.x, v, a.idx) : search_sorted_last(a.x, v, a.idx)], a)
.>={T}(a::IndexedVec{T}, v::T) = Indexer(Range1[search_sorted_first(a.x, v, a.idx) : length(a)], a)
.<={T}(a::IndexedVec{T}, v::T) = Indexer(Range1[1 : search_sorted_last(a.x, v, a.idx)], a)
.>={T}(v::T, a::IndexedVec{T}) = Indexer(Range1[1 : search_sorted_last(a.x, v, a.idx)], a)
.<={T}(v::T, a::IndexedVec{T}) = Indexer(Range1[search_sorted_first(a.x, v, a.idx) : length(a)], a)
.>{T}(a::IndexedVec{T}, v::T) = Indexer(Range1[search_sorted_first_gt(a.x, v, a.idx) : length(a)], a)
.<{T}(a::IndexedVec{T}, v::T) = Indexer(Range1[1 : search_sorted_last_lt(a.x, v, a.idx)], a)
.<{T}(v::T, a::IndexedVec{T}) = Indexer(Range1[search_sorted_first_gt(a.x, v, a.idx) : length(a)], a)
.>{T}(v::T, a::IndexedVec{T}) = Indexer(Range1[1 : search_sorted_last_lt(a.x, v, a.idx)], a)


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

function in{T}(a::IndexedVec{T}, y::Vector{T})
    res = a .== y[1]
    for i in 2:length(y)
        res = res | (a .== y[i])
    end
    res
end

between{T}(a::IndexedVec{T}, v1::T, v2::T, ) = Indexer(Range1[search_sorted_first(a.x, v1, a.idx) : search_sorted_last(a.x, v2, a.idx)], a)

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

# the following was needed for show(df)
maxShowLength(v::IndexedVec) = length(v) > 0 ? max([length(_string(x)) for x = v.x]) : 0


## # Examples
## srand(1)
## a = randi(5,20)
## ia = IndexedVec(a)
## ia2 = IndexedVec(randi(4,20))
## ia .== 4
## v = [1:20]
## v[ia .== 4]

## (ia .== 4) | (ia .== 5)

## v[(ia .== 4) | (ia .== 5)]

## v[(ia .>= 4) | (ia .== 5)] | println
## ia[(ia .>= 4) | (ia .== 5)] | println
## v[(ia .>= 4) & (ia .== 5)] | println
## ia[(ia .>= 4) & (ia .== 5)] | println

## !(ia .== 4) | dump
## ia[!(ia .== 4)] | println

## (ia .== 4) | dump 
## (ia .== 4) & (ia .>= 3) | dump 

## (ia .== 4) | (ia .== 3) | dump 
## (ia .== 4) | (ia .== 3) | (ia .== 1) | dump



## df = DataFrame({IndexedVec(vcat(fill([1:5],4)...)), IndexedVec(vcat(fill(letters[1:10],2)...))})

## df[:(x2 .== "a")] 
## df[:( (x2 .== "a") | (x1 .== 2) )] 
## df[:( ("b" .<= x2 .<= "c") | (x1 .== 5) )]
## df[:( (x1 .== 1) & (x2 .== "a") )]

## df[:( in(x2, ["c","e"]) )]
