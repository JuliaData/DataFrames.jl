# This is an experiment in adding indexing to vectors. The idea is
# that if a DataFrame column is indexed, the following will have fast
# lookups:
#     df[:(2 .< col1 .< 7), :]
#     df[:( datecol .>= "2011-01-01" ), "col3"]
#     df[:( col .== "red" ), :]
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
#       df[:( keycol .== "A" ), :]. (But, df["A"] is less descriptive if
#       you don't know what df's keys are.)



# An IndexedVector is a pointer to the original column along with an
# indexing vector equal to `order(orig)`. A comparison operation like
# `idv .> 3` returns an Indexer type. The Indexer type includes a
# pointer to the IndexedVector along with a vector of Range1's.
# DataVector's and DataFrame's can be indexed with Indexers. It's fast
# because you're using a slice of the already indexed vector.

# Indexer's can be combined with `|` and `&`. In the case where the
# IndexedVector is the same, the Indexer is reduced or expanded as
# appropriate. This includes: `0 .< idv .< 3` or `idv .== 1 | idv .==
# 4`. If Indexers have different IndexedVectors (like `idv1 .== 1 |
# idv2 .== 1`), then the result is converted to a BitVector.

# We handle NA's by excluding them from the index. `order` puts NA's
# at the front. We use the following function to exclude those. This
# does make the indexing a little trickier as the length of the index
# can be less than the length of the DataArray.


# Note that the following leaves out NA's.
indexorder(x) = sortperm(x)
function indexorder(v::AbstractDataVector)
    Nna = sum(isna(v))
    sortperm(v)[Nna + 1 : end]
end


type IndexedVector{T,S<:AbstractVector} <: AbstractVector{T}
    x::S
    idx::Vector{Int}
end
IndexedVector{T}(x::AbstractVector{T}) = IndexedVector{T,typeof(x)}(x, indexorder(x))

getindex{I<:Real}(v::IndexedVector,i::AbstractVector{I}) = IndexedVector(v.x[i])
getindex{I<:Real}(v::IndexedVector,i::I) = v.x[i]
getindex(v::IndexedVector,i::Real) = v.x[i]
setindex!(v::IndexedVector, val::Any, i::Real) = IndexedVector(setindex!(v.x, val, i))
setindex!(v::IndexedVector, val::Any, inds::AbstractVector) = IndexedVector(setindex!(v.x, val, inds))
reverse(v::IndexedVector) = IndexedVector(reverse(v.x), reverse(v.idx))
similar(v::IndexedVector, T, dims::Dims) = similar(v.x, T, dims)

vecbind_type(x::IndexedVector) = vecbind_type(x.x)

# to make assign in a DataFrame work:
upgrade_vector(v::IndexedVector) = v


sortperm{S,A<:AbstractDataVector}(x::IndexedVector{S,A}) = [findin(x.x, [NA]), x.idx]
sortperm(x::IndexedVector) = x.idx 
sortperm(x::IndexedVector, ::Sort.ReverseOrdering) = reverse(x.idx)
sort(x::IndexedVector) = x.x[sortperm(x)]
sort(x::IndexedVector, ::Sort.ReverseOrdering) = x.x[reverse(sortperm(x))]
Perm{O<:Sort.Ordering}(o::O, v::IndexedVector) = FastPerm(o, v)

type Indexer
    r::Vector{Range1}
    iv::IndexedVector
    function Indexer(r::Vector{Range1}, iv::IndexedVector)
        for i in 1:length(r)
            ri = r[i]
            if length(ri) < 1 || ri[1] < 1 || ri[end] > length(iv)
                delete!(r, i)
            end
        end
        new(r, iv)
    end
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
                push!(res, max(r1[1], r2[1]):min(r1[end], r2[end]))
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
            push!(res, pop!(right))
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
                pop!(right)
            end
        end
        if overlap
            if length(right) == 0 && length(left) > 0 && r_start <= left[end][1]
                r_start = left[end][1]
                pop!(left)
            end
            push!(res, r_start : r_end)
        end
    end
    # Rest of v1 or v2 (no overlaps here)
    while length(v1) > 0
        push!(res, pop!(v1))
    end
    while length(v2) > 0
        push!(res, pop!(v2))
    end
    reverse(res)
end

function (!)(x::Indexer)   # Negate the Indexer
    res = Range1[1 : x.r[1][1] - 1]
    for i in 1:length(x.r) - 1
        push!(res, x.r[i][end] + 1 : x.r[i + 1][1] - 1)
    end
    push!(res, x.r[end][end] + 1 : length(x.iv.idx))
    Indexer(res, x.iv)
end

function (&)(x1::Indexer, x2::Indexer)
    if is(x1.iv, x2.iv)
        Indexer(intersect(x1.r, x2.r), x1.iv)
    else
        bool(x1) & bool(x2)
    end
end
(&)(x1::BitVector, x2::Indexer) = x1 & bool(x2)
(&)(x1::Indexer, x2::BitVector) = x2 & bool(x1)

function (|)(x1::Indexer, x2::Indexer)
    if is(x1.iv, x2.iv)
        Indexer(union(x1.r, x2.r), x1.iv)
    else
        bool(x1) | bool(x2)
    end
end
(|)(x1::BitVector, x2::Indexer) = x1 | bool(x2)
(|)(x1::Indexer, x2::BitVector) = x2 | bool(x1)

function bool(ix::Indexer)
    res = falses(length(ix.iv))
    for i in ix.iv.idx[[ix.r...]]
        res[i] = true
    end
    res
end

# `getindex` -- each Range1 of the Indexer (i.r...) is applied to the indexing vector (i.iv.idx)

getindex(x::IndexedVector, i::Indexer) = x[i.iv.idx[[i.r...]]]
getindex(x::AbstractVector, i::Indexer) = x[i.iv.idx[[i.r...]]]
getindex(x::AbstractDataVector, i::Indexer) = x[i.iv.idx[[i.r...]]]

# df[MultiRowIndex, SingleColumnIndex] => (Sub)?AbstractDataVector
function getindex(df::DataFrame, row_inds::Indexer, col_ind::ColumnIndex)
    selected_column = df.colindex[col_ind]
    return df.columns[selected_column][row_inds.iv.idx[[row_inds.r...]]]
end

# df[MultiRowIndex, MultiColumnIndex] => (Sub)?DataFrame
function getindex{T <: ColumnIndex}(df::DataFrame, row_inds::Indexer, col_inds::AbstractVector{T})
    selected_columns = df.colindex[col_inds]
    new_columns = {dv[row_inds.iv.idx[[row_inds.r...]]] for dv in df.columns[selected_columns]}
    return DataFrame(new_columns, Index(df.colindex.names[selected_columns]))
end

typealias ComparisonTypes Union(Number, String)   # defined mainly to avoid warnings

# element-wise (in)equality operators
# these may need range checks
# Should these results be sorted? Could be a counting sort.
.=={T<:ComparisonTypes}(a::IndexedVector{T}, v::T) = Indexer(Range1[search_sorted_first(a.x, v, a.idx) : search_sorted_last(a.x, v, a.idx)], a)
.=={T<:ComparisonTypes}(v::T, a::IndexedVector{T}) = Indexer(Range1[search_sorted_first(a.x, v, a.idx) : search_sorted_last(a.x, v, a.idx)], a)
.>={T<:ComparisonTypes}(a::IndexedVector{T}, v::T) = Indexer(Range1[search_sorted_first(a.x, v, a.idx) : length(a.idx)], a)
.<={T<:ComparisonTypes}(a::IndexedVector{T}, v::T) = Indexer(Range1[1 : search_sorted_last(a.x, v, a.idx)], a)
.>={T<:ComparisonTypes}(v::T, a::IndexedVector{T}) = Indexer(Range1[1 : search_sorted_last(a.x, v, a.idx)], a)
.<={T<:ComparisonTypes}(v::T, a::IndexedVector{T}) = Indexer(Range1[search_sorted_first(a.x, v, a.idx) : length(a.idx)], a)
.>{T<:ComparisonTypes}(a::IndexedVector{T}, v::T) = Indexer(Range1[search_sorted_first_gt(a.x, v, a.idx) : length(a.idx)], a)
.<{T<:ComparisonTypes}(a::IndexedVector{T}, v::T) = Indexer(Range1[1 : search_sorted_last_lt(a.x, v, a.idx)], a)
.<{T<:ComparisonTypes}(v::T, a::IndexedVector{T}) = Indexer(Range1[search_sorted_first_gt(a.x, v, a.idx) : length(a.idx)], a)
.>{T<:ComparisonTypes}(v::T, a::IndexedVector{T}) = Indexer(Range1[1 : search_sorted_last_lt(a.x, v, a.idx)], a)


function search_sorted_first_gt{I<:Integer}(a::AbstractVector, x, idx::AbstractVector{I})
    res = search_sorted_last(a, x, idx)
    if res == 0 return 1 end
    if res == length(a) && a[idx[res]] != x return(length(a)+1) end 
    a[idx[res]] == x ? res + 1 : res
end
function search_sorted_last_lt{I<:Integer}(a::AbstractVector, x, idx::AbstractVector{I})
    res = search_sorted_first(a, x, idx)
    if res > length(idx) return length(idx) end
    if res == 1 && a[idx[res]] != x return(0) end 
    a[idx[res]] == x ? res - 1 : res
end

findin(a::IndexedVector,r::Range1) = findin(a, [r])
findin(a::IndexedVector,v::Real) = findin(a, [v])
function findin(a::IndexedVector, b::AbstractVector)
    ## Returns an Indexer with the elements in "a" that appear in "b"
    res = a .== b[1]
    for i in 2:length(b)
        res = res | (a .== b[i])
    end
    res
end
        
size(a::IndexedVector) = size(a.x)
length(a::IndexedVector) = length(a.x)
ndims(a::IndexedVector) = 1
eltype(a::IndexedVector) = eltype(a.x)

## print(io, a::IndexedVector) = print(io, a.x)
function show(io::IO, a::IndexedVector)
    print(io, "IndexedVector: ")
    show(io, a.x)
end
function repl_show(io::IO, a::IndexedVector)
    print(io, "IndexedVector: ")
    repl_show(io, a.x)
end

function search_sorted_last{I<:Integer}(a::AbstractVector, x, idx::AbstractVector{I})
    ## Index of the last value of vector a that is less than or equal to x.
    ## Returns 0 if x is less than all values of a.
    ## idx is an indexing vector equal in length to a that sorts a 
    ## @assert length(a) == length(idx)
    lo = 0
    hi = length(idx) + 1
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
    ## @assert length(a) == length(idx)
    lo = 0
    hi = length(idx) + 1
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



# the following is needed for show(df)
maxShowLength(v::IndexedVector) = length(v) > 0 ? max([length(_string(x)) for x = v.x]) : 0

# Methods to speed up grouping and merging
function PooledDataArray{R}(d::IndexedVector, ::Type{R})
    refs = zeros(R, size(d))
    oneval = one(R)
    local idx::Int
    ## local lastval::T
    local poolidx::R
    pool = Array(eltype(d), 0)
    # skip over NAs
    nna = length(d) - length(d.x)
    if nna == length(d)
        return PooledDataArray(RefArray(refs), pool)
    end
    lastval = d.x[d.idx[nna+1]]
    push!(pool, d.x[d.idx[nna+1]])
    poolidx = oneval
    for i = 1 : length(d.idx)
        idx = d.idx[i]
        val = d.x[idx]
        if val != lastval
            push!(pool, val)
            poolidx += oneval
            lastval = val
        end
        refs[idx] = poolidx
    end
    return PooledDataArray(RefArray(refs), pool)
end
PooledDataArray(d::IndexedVector) = PooledDataArray(d, DEFAULT_POOLED_REF_TYPE)

DataArray(d::IndexedVector) = DataArray(x.x)

function PooledDataVecs{S}(v1::IndexedVector{S},
                           v2::IndexedVector{S})
    return PooledDataVecs(PooledDataArray(v1),
                          PooledDataArray(v2))
end
