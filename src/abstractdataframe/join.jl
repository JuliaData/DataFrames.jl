@comment """
# Joins
"""

##
## Join / merge
##

function join_idx(left, right, max_groups)
    ## adapted from Wes McKinney's full_outer_join in pandas (file: src/join.pyx).

    # NA group in location 0

    left_sorter, where, left_count = DataArrays.groupsort_indexer(left, max_groups)
    right_sorter, where, right_count = DataArrays.groupsort_indexer(right, max_groups)

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

    # group 0 is the NA group
    tposition = 0
    lposition = 0
    rposition = 0

    left_pos = 0
    right_pos = 0

    left_indexer = Array(Int, tcount)
    right_indexer = Array(Int, tcount)
    leftonly_indexer = Array(Int, lcount)
    rightonly_indexer = Array(Int, rcount)
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

function DataArrays.PooledDataVecs(df1::AbstractDataFrame,
                                   df2::AbstractDataFrame)
    # This method exists to allow merge to work with multiple columns.
    # It takes the columns of each DataFrame and returns a DataArray
    # with a merged pool that "keys" the combination of column values.
    # The pools of the result don't really mean anything.
    dv1, dv2 = PooledDataVecs(df1[1], df2[1])
    refs1 = dv1.refs .+ 1   # the + 1 handles NA's
    refs2 = dv2.refs .+ 1
    ngroups = length(dv1.pool) + 1
    for j = 2:ncol(df1)
        dv1, dv2 = PooledDataVecs(df1[j], df2[j])
        for i = 1:length(refs1)
            refs1[i] += (dv1.refs[i]) * ngroups
        end
        for i = 1:length(refs2)
            refs2[i] += (dv2.refs[i]) * ngroups
        end
        # FIXME check for ngroups overflow, maybe recode refs to prevent it
        ngroups *= (length(dv1.pool) + 1)
    end
    # recode refs1 and refs2 to drop the unused column combinations and
    # limit the pool size
    PooledDataVecs( refs1, refs2 )
end

function DataArrays.PooledDataArray{R}(df::AbstractDataFrame, ::Type{R})
    # This method exists to allow another way for merge to work with
    # multiple columns. It takes the columns of the DataFrame and
    # returns a DataArray with a merged pool that "keys" the
    # combination of column values.
    # Notes:
    #   - I skipped the sort to make it faster.
    #   - Converting each individual one-row DataFrame to a Tuple
    #     might be faster.
    refs = zeros(R, nrow(df))
    poolref = Dict{AbstractDataFrame, Int}()
    pool = Array(UInt64, 0)
    j = 1
    for i = 1:nrow(df)
        val = df[i,:]
        if haskey(poolref, val)
            refs[i] = poolref[val]
        else
            push!(pool, hash(val))
            refs[i] = j
            poolref[val] = j
            j += 1
        end
    end
    return PooledDataArray(DataArrays.RefArray(refs), pool)
end

DataArrays.PooledDataArray(df::AbstractDataFrame) = PooledDataArray(df, DEFAULT_POOLED_REF_TYPE)



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

`NA`s are filled in where needed to complete joins.

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

    dv1, dv2 = PooledDataVecs(df1[on], df2[on])

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
                     nas(df2w, length(leftonly_idx)))

        return hcat!(left, right)
    elseif kind == :right
        df1w = without(df1, on)

        left = vcat(df1w[left_idx, :],
                    nas(df1w, length(rightonly_idx)))
        right = df2[[right_idx; rightonly_idx], :]

        return hcat!(left, right)
    elseif kind == :outer
        df1w, df2w = without(df1, on),  without(df2, on)

        mixed = hcat!(df1[left_idx, :], df2w[right_idx, :])
        leftonly = hcat!(df1[leftonly_idx, :],
                         nas(df2w, length(leftonly_idx)))
        rightonly = hcat!(nas(df1w, length(rightonly_idx)),
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
    cols = [[rep(c, 1, r2) for c in columns(df1)];
            [rep(c, r1, 1) for c in columns(df2)]]
    colindex = merge(index(df1), index(df2))
    DataFrame(cols, colindex)
end
