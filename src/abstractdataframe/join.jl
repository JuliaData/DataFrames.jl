##
## Join / merge
##

# Like similar, but returns a nullable array
similar_nullable{T}(dv::AbstractArray{T}, dims::@compat(Union{Int, Tuple{Vararg{Int}}})) =
    NullableArray(T, dims)

similar_nullable{T<:Nullable}(dv::AbstractArray{T}, dims::@compat(Union{Int, Tuple{Vararg{Int}}})) =
    NullableArray(eltype(T), dims)

similar_nullable{T,R}(dv::CategoricalArray{T,R}, dims::@compat(Union{Int, Tuple{Vararg{Int}}})) =
    NullableCategoricalArray(T, dims)

similar_nullable(df::AbstractDataFrame, dims::Int) =
    DataFrame(Any[similar_nullable(x, dims) for x in columns(df)], copy(index(df)))

function join_idx(left, right, max_groups)
    ## adapted from Wes McKinney's full_outer_join in pandas (file: src/join.pyx).

    # NULL group in location 0

    left_sorter, where, left_count = groupsort_indexer(left, max_groups)
    right_sorter, where, right_count = groupsort_indexer(right, max_groups)

    # First pass, determine size of result set
    tcount = 0
    rcount = 0
    lcount = 0
    for i in 1:(max_groups + 1)
        lc = left_count[i]
        rc = right_count[i]

        if rc > 0 && lc > 0
            tcount += lc * rc
        elseif rc > 0
            rcount += rc
        else
            lcount += lc
        end
    end

    # group 0 is the NULL group
    tposition = 0
    lposition = 0
    rposition = 0

    left_pos = 0
    right_pos = 0

    left_indexer = Vector{Int}(tcount)
    right_indexer = Vector{Int}(tcount)
    leftonly_indexer = Vector{Int}(lcount)
    rightonly_indexer = Vector{Int}(rcount)
    for i in 1:(max_groups + 1)
        lc = left_count[i]
        rc = right_count[i]
        if rc == 0
            for j in 1:lc
                leftonly_indexer[lposition + j] = left_pos + j
            end
            lposition += lc
        elseif lc == 0
            for j in 1:rc
                rightonly_indexer[rposition + j] = right_pos + j
            end
            rposition += rc
        else
            for j in 1:lc
                offset = tposition + (j-1) * rc
                for k in 1:rc
                    left_indexer[offset + k] = left_pos + j
                    right_indexer[offset + k] = right_pos + k
                end
            end
            tposition += lc * rc
        end
        left_pos += lc
        right_pos += rc
    end

    ## (left_sorter, left_indexer, leftonly_indexer,
    ##  right_sorter, right_indexer, rightonly_indexer)
    (left_sorter[left_indexer], left_sorter[leftonly_indexer],
     right_sorter[right_indexer], right_sorter[rightonly_indexer])
end

function sharepools{S,N}(v1::Union{CategoricalArray{S,N}, NullableCategoricalArray{S,N}},
                         v2::Union{CategoricalArray{S,N}, NullableCategoricalArray{S,N}},
                         index::Vector{S},
                         R)
    tidx1 = convert(Vector{R}, indexin(CategoricalArrays.index(v1.pool), index))
    tidx2 = convert(Vector{R}, indexin(CategoricalArrays.index(v2.pool), index))
    refs1 = zeros(R, length(v1))
    refs2 = zeros(R, length(v2))
    for i in 1:length(refs1)
        if v1.refs[i] != 0
            refs1[i] = tidx1[v1.refs[i]]
        end
    end
    for i in 1:length(refs2)
        if v2.refs[i] != 0
            refs2[i] = tidx2[v2.refs[i]]
        end
    end
    pool = CategoricalPool{S, R}(index)
    return (CategoricalArray(refs1, pool),
            CategoricalArray(refs2, pool))
end

function sharepools{S,N}(v1::Union{CategoricalArray{S,N}, NullableCategoricalArray{S,N}},
                         v2::Union{CategoricalArray{S,N}, NullableCategoricalArray{S,N}})
    index = sort(unique([levels(v1); levels(v2)]))
    sz = length(index)

    R = sz <= typemax(UInt8)  ? UInt8 :
        sz <= typemax(UInt16) ? UInt16 :
        sz <= typemax(UInt32) ? UInt32 :
                                UInt64

    # To ensure type stability during actual work
    sharepools(v1, v2, index, R)
end

sharepools{S,N}(v1::Union{CategoricalArray{S,N}, NullableCategoricalArray{S,N}},
                v2::AbstractArray{S,N}) =
    sharepools(v1, oftype(v1, v2))

sharepools{S,N}(v1::AbstractArray{S,N},
                v2::Union{CategoricalArray{S,N}, NullableCategoricalArray{S,N}}) =
    sharepools(oftype(v2, v1), v2)

# TODO: write an optimized version for (Nullable)CategoricalArray
function sharepools{S, T}(v1::AbstractArray{S},
                          v2::AbstractArray{T})
    ## Return two categorical arrays that share the same pool.

    ## TODO: allow specification of R
    R = CategoricalArrays.DefaultRefType
    refs1 = Array{R}(size(v1))
    refs2 = Array{R}(size(v2))
    K = promote_type(S, T)
    poolref = Dict{K, R}()
    maxref = 0

    # loop through once to fill the poolref dict
    for i = 1:length(v1)
        if !_isnull(v1[i])
            poolref[K(v1[i])] = 0
        end
    end
    for i = 1:length(v2)
        if !_isnull(v2[i])
            poolref[K(v2[i])] = 0
        end
    end

    # fill positions in poolref
    pool = sort(collect(keys(poolref)))
    i = 1
    for p in pool
        poolref[p] = i
        i += 1
    end

    # fill in newrefs
    zeroval = zero(R)
    for i = 1:length(v1)
        if _isnull(v1[i])
            refs1[i] = zeroval
        else
            refs1[i] = poolref[K(v1[i])]
        end
    end
    for i = 1:length(v2)
        if _isnull(v2[i])
            refs2[i] = zeroval
        else
            refs2[i] = poolref[K(v2[i])]
        end
    end

    pool = CategoricalPool(pool)
    return (NullableCategoricalArray(refs1, pool),
            NullableCategoricalArray(refs2, pool))
end

function sharepools(df1::AbstractDataFrame, df2::AbstractDataFrame)
    # This method exists to allow merge to work with multiple columns.
    # It takes the columns of each DataFrame and returns a categorical array
    # with a merged pool that "keys" the combination of column values.
    # The pools of the result don't really mean anything.
    dv1, dv2 = sharepools(df1[1], df2[1])
    # use UInt32 instead of the minimum integer size chosen by sharepools
    # since the number of levels can be high
    refs1 = Vector{UInt32}(dv1.refs)
    refs2 = Vector{UInt32}(dv2.refs)
    # the + 1 handles nulls
    refs1[:] += 1
    refs2[:] += 1
    ngroups = length(levels(dv1)) + 1
    for j = 2:ncol(df1)
        dv1, dv2 = sharepools(df1[j], df2[j])
        for i = 1:length(refs1)
            refs1[i] += (dv1.refs[i]) * ngroups
        end
        for i = 1:length(refs2)
            refs2[i] += (dv2.refs[i]) * ngroups
        end
        ngroups *= length(levels(dv1)) + 1
    end
    # recode refs1 and refs2 to drop the unused column combinations and
    # limit the pool size
    sharepools(refs1, refs2)
end


"""
Join two DataFrames

```julia
join(df1::AbstractDataFrame,
     df2::AbstractDataFrame;
     on::Union{Symbol, Vector{Symbol}} = Symbol[],
     kind::Symbol = :inner)
```

### Arguments

* `df1`, `df2` : the two AbstractDataFrames to be joined

### Keyword Arguments

* `on` : a Symbol or Vector{Symbol}, the column(s) used as keys when
  joining; required argument except for `kind = :cross`

* `kind` : the type of join, options include:

  - `:inner` : only include rows with keys that match in both `df1`
    and `df2`, the default
  - `:outer` : include all rows from `df1` and `df2`
  - `:left` : include all rows from `df1`
  - `:right` : include all rows from `df2`
  - `:semi` : return rows of `df1` that match with the keys in `df2`
  - `:anti` : return rows of `df1` that do not match with the keys in `df2`
  - `:cross` : a full Cartesian product of the key combinations; every
    row of `df1` is matched with every row of `df2`

Null values are filled in where needed to complete joins.

### Result

* `::DataFrame` : the joined DataFrame

### Examples

```julia
name = DataFrame(ID = [1, 2, 3], Name = ["John Doe", "Jane Doe", "Joe Blogs"])
job = DataFrame(ID = [1, 2, 4], Job = ["Lawyer", "Doctor", "Farmer"])

join(name, job, on = :ID)
join(name, job, on = :ID, kind = :outer)
join(name, job, on = :ID, kind = :left)
join(name, job, on = :ID, kind = :right)
join(name, job, on = :ID, kind = :semi)
join(name, job, on = :ID, kind = :anti)
join(name, job, kind = :cross)
```

"""
function Base.join(df1::AbstractDataFrame,
                   df2::AbstractDataFrame;
                   on::@compat(Union{Symbol, Vector{Symbol}}) = Symbol[],
                   kind::Symbol = :inner)
    if kind == :cross
        if on != Symbol[]
            throw(ArgumentError("Cross joins don't use argument 'on'."))
        end
        return crossjoin(df1, df2)
    elseif on == Symbol[]
        throw(ArgumentError("Missing join argument 'on'."))
    end

    dv1, dv2 = sharepools(df1[on], df2[on])

    left_idx, leftonly_idx, right_idx, rightonly_idx =
        join_idx(dv1.refs, dv2.refs, length(dv1.pool))

    if kind == :inner
        df2w = without(df2, on)

        left = df1[left_idx, :]
        right = df2w[right_idx, :]

        return hcat!(left, right)
    elseif kind == :left
        df2w = without(df2, on)

        left = df1[[left_idx; leftonly_idx], :]
        right = vcat(df2w[right_idx, :],
                     similar_nullable(df2w, length(leftonly_idx)))

        return hcat!(left, right)
    elseif kind == :right
        df1w = without(df1, on)

        left = vcat(df1w[left_idx, :],
                    similar_nullable(df1w, length(rightonly_idx)))
        right = df2[[right_idx; rightonly_idx], :]

        return hcat!(left, right)
    elseif kind == :outer
        df1w, df2w = without(df1, on),  without(df2, on)

        mixed = hcat!(df1[left_idx, :], df2w[right_idx, :])
        leftonly = hcat!(df1[leftonly_idx, :],
                         similar_nullable(df2w, length(leftonly_idx)))
        rightonly = hcat!(similar_nullable(df1w, length(rightonly_idx)),
                          df2[rightonly_idx, :])

        return vcat(mixed, leftonly, rightonly)
    elseif kind == :semi
        df1[unique(left_idx), :]
    elseif kind == :anti
        df1[leftonly_idx, :]
    else
        throw(ArgumentError("Unknown kind of join requested"))
    end
end

function crossjoin(df1::AbstractDataFrame, df2::AbstractDataFrame)
    r1, r2 = size(df1, 1), size(df2, 1)
    cols = Any[[Compat.repeat(c, inner=r2) for c in columns(df1)];
            [Compat.repeat(c, outer=r1) for c in columns(df2)]]
    colindex = merge(index(df1), index(df2))
    DataFrame(cols, colindex)
end
