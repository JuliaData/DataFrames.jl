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

indexorder(x) = sortperm(x)
function indexorder{T}(v::AbstractDataVector{T})
    Nna = sum(isna(v))
    sortperm(v)[Nna + 1 : end]
end


type IndexedVector{T} <: AbstractVector{T}
    x::AbstractVector{T}
    idx::Vector{Int}
end
IndexedVector(x::AbstractVector) = IndexedVector(x, indexorder(x))

ref{T,I<:Real}(v::IndexedVector{T},i::AbstractVector{I}) = IndexedVector(v.x[i])
ref{T}(v::IndexedVector{T},i::Real) = v.x[i]
ref{T}(v::IndexedVector{T}, i) = IndexedVector(v.x[i])
assign(v::IndexedVector, i, val::Real) = IndexedVector(assign(v.x, i, val))
assign(v::IndexedVector, i, val) = IndexedVector(assign(v.x, i, val))

# to make assign in a DataFrame work:
upgrade_vector{T}(v::IndexedVector{T}) = v
function insert_single_column!{T}(df::DataFrame,
                               dv::IndexedVector{T},
                               col_ind::ColumnIndex)
    dv_n, df_n = length(dv), nrow(df)
    if df_n != 0
        if dv_n != df_n
            #dv = repeat(dv, df_n)
            error("New columns must have the same length as old columns")
        end
    end
    if has(df.colindex, col_ind)
        j = df.colindex[col_ind]
        df.columns[j] = dv
    else
        if typeof(col_ind) <: String || typeof(col_ind) <: Symbol
            push!(df.colindex, col_ind)
            push!(df.columns, dv)
        else
            if isnextcol(df, col_ind)
                push!(df.colindex, nextcolname(df))
                push!(df.columns, dv)
            else
                println("Column does not exist: $col_ind")
                error("Cannot assign to non-existent column")
            end
        end
    end
    return dv
end


sortperm(x::IndexedVector) = x.idx
sort(x::IndexedVector) = x.x[x.idx]   # Return regular array?
## sort(x::IndexedVector) = IndexedVector(x.x[x.idx], 1:length(x.x))   # or keep this an IndexedVector, like this?

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

# `ref` -- each Range1 of the Indexer (i.r...) is applied to the indexing vector (i.iv.idx)

ref(x::IndexedVector, i::Indexer) = x[i.iv.idx[[i.r...]]]
ref(x::AbstractVector, i::Indexer) = x[i.iv.idx[[i.r...]]]
ref(x::AbstractDataVector, i::Indexer) = x[i.iv.idx[[i.r...]]]

# df[MultiRowIndex, SingleColumnIndex] => (Sub)?AbstractDataVector
function ref(df::DataFrame, row_inds::Indexer, col_ind::ColumnIndex)
    selected_column = df.colindex[col_ind]
    return df.columns[selected_column][row_inds.iv.idx[[row_inds.r...]]]
end

# df[MultiRowIndex, MultiColumnIndex] => (Sub)?DataFrame
function ref{T <: ColumnIndex}(df::DataFrame, row_inds::Indexer, col_inds::AbstractVector{T})
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
eltype{T}(a::IndexedVector{T}) = T

## print(io, a::IndexedVector) = print(io, a.x)
show(io::IO, a::IndexedVector) = show(io, a.x)
repl_show(io::IO, a::IndexedVector) = repl_show(io, a.x)

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

