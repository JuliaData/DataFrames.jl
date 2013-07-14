##
## Join / merge
##

function join_idx(left, right, max_groups)
    ## adapted from Wes McKinney's full_outer_join in pandas (file: src/join.pyx).

    # NA group in location 0

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

function PooledDataVecs(df1::AbstractDataFrame,
                        df2::AbstractDataFrame)
    # This method exists to allow merge to work with multiple columns.
    # It takes the columns of each DataFrame and returns a DataArray
    # with a merged pool that "keys" the combination of column values.
    # The pools of the result don't really mean anything.
    dv1, dv2 = PooledDataVecs(df1[1], df2[1])
    refs1 = dv1.refs + 1   # the + 1 handles NA's
    refs2 = dv2.refs + 1
    ngroups = length(dv1.pool) + 1
    for j = 2:ncol(df1)
        dv1, dv2 = PooledDataVecs(df1[j], df2[j])
        for i = 1:length(refs1)
            refs1[i] += (dv1.refs[i]) * ngroups
        end
        for i = 1:length(refs2)
            refs2[i] += (dv2.refs[i]) * ngroups
        end
        ngroups = ngroups * (length(dv1.pool) + 1)
    end
    pool = [1:ngroups]
    (PooledDataArray(RefArray(refs1), pool), PooledDataArray(RefArray(refs2), pool))
end

function PooledDataArray{R}(df::AbstractDataFrame, ::Type{R})
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
    pool = Array(Uint64, 0)
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
    return PooledDataArray(RefArray(refs), pool)
end

PooledDataArray(df::AbstractDataFrame) = PooledDataArray(df, DEFAULT_POOLED_REF_TYPE)

# Union(Vector{T}, ByteString, Nothing
function Base.join(df1::AbstractDataFrame,
                   df2::AbstractDataFrame;
                   on::Any = nothing,
                   kind::Symbol = :inner)
    if on == nothing
        on = first(collect(intersect(Set{ByteString}(colnames(df1)...),
                                     Set{ByteString}(colnames(df2)...))))
    end
    dv1, dv2 = PooledDataVecs(df1[on], df2[on])
    left_indexer, leftonly_indexer, right_indexer, rightonly_indexer =
        join_idx(dv1.refs, dv2.refs, length(dv1.pool))

    if kind == :inner
        return cbind(df1[left_indexer,:], without(df2, on)[right_indexer,:])
    elseif kind == :left
        left = df1[[left_indexer,leftonly_indexer],:]
        right = rbind(without(df2, on)[right_indexer,:],
                      nas(without(df2, on), length(leftonly_indexer)))
        return cbind(left, right)
    elseif kind == :right
        left = rbind(without(df1, on)[left_indexer, :],
                     nas(without(df1, on), length(rightonly_indexer)))
        right = df2[[right_indexer,rightonly_indexer],:]
        return cbind(left, right)
    elseif kind == :outer
        mixed = cbind(df1[left_indexer, :], without(df2, on)[right_indexer, :])
        leftonly = cbind(df1[leftonly_indexer, :],
                         nas(without(df2, on), length(leftonly_indexer)))
        leftonly = leftonly[:, colnames(mixed)]
        rightonly = cbind(nas(without(df1, on), length(rightonly_indexer)),
                          df2[rightonly_indexer, :])
        rightonly = rightonly[:, colnames(mixed)]
        return rbind(mixed, leftonly, rightonly)
    else
        throw(ArgumentError("Unknown kind of join requested"))
    end
end
