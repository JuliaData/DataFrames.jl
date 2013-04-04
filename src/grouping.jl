#
#  Split - Apply - Combine operations
#

function groupsort_indexer(x::AbstractVector, ngroups::Int)
    ## translated from Wes McKinney's groupsort_indexer in pandas (file: src/groupby.pyx).

    ## count group sizes, location 0 for NA
    n = length(x)
    ## counts = x.pool
    counts = fill(0, ngroups + 1)
    for i = 1:n
        counts[x[i] + 1] += 1
    end

    ## mark the start of each contiguous group of like-indexed data
    where = fill(1, ngroups + 1)
    for i = 2:ngroups+1
        where[i] = where[i - 1] + counts[i - 1]
    end
    
    ## this is our indexer
    result = fill(0, n)
    for i = 1:n
        label = x[i] + 1
        result[where[label]] = i
        where[label] += 1
    end
    result, where, counts
end
groupsort_indexer(pv::PooledDataVector) = groupsort_indexer(pv.refs, length(pv.pool))

##############################################################################
##
## GroupedDataFrame...
##
##############################################################################

type GroupedDataFrame
    parent::AbstractDataFrame
    cols::Vector         # columns used for sorting
    idx::Vector{Int}     # indexing vector when sorted by the given columns
    starts::Vector{Int}  # starts of groups
    ends::Vector{Int}    # ends of groups 
end

#
# Split
#
function groupby{T}(df::AbstractDataFrame, cols::Vector{T})
    ## a subset of Wes McKinney's algorithm here:
    ##     http://wesmckinney.com/blog/?p=489
    
    # use the pool trick to get a set of integer references for each unique item
    dv = PooledDataArray(df[cols[1]])
    # if there are NAs, add 1 to the refs to avoid underflows in x later
    dv_has_nas = (findfirst(dv.refs, 0) > 0 ? 1 : 0)
    x = copy(dv.refs) + dv_has_nas
    # also compute the number of groups, which is the product of the set lengths
    ngroups = length(dv.pool) + dv_has_nas
    # if there's more than 1 column, do roughly the same thing repeatedly
    for j = 2:length(cols)
        dv = PooledDataArray(df[cols[j]])
        dv_has_nas = (findfirst(dv.refs, 0) > 0 ? 1 : 0)
        for i = 1:nrow(df)
            x[i] += (dv.refs[i] + dv_has_nas- 1) * ngroups
        end
        ngroups = ngroups * (length(dv.pool) + dv_has_nas)
        # TODO if ngroups is really big, shrink it
    end
    (idx, starts) = groupsort_indexer(x, ngroups)
    # Remove zero-length groupings
    starts = _uniqueofsorted(starts) 
    ends = [starts[2:end] - 1]
    GroupedDataFrame(df, cols, idx, starts[1:end-1], ends)
end
groupby(d::AbstractDataFrame, cols) = groupby(d, [cols])

# add a function curry
groupby{T}(cols::Vector{T}) = x -> groupby(x, cols)
groupby(cols) = x -> groupby(x, cols)

start(gd::GroupedDataFrame) = 1
next(gd::GroupedDataFrame, state::Int) = 
    (sub(gd.parent, gd.idx[gd.starts[state]:gd.ends[state]]),
     state + 1)
done(gd::GroupedDataFrame, state::Int) = state > length(gd.starts)
length(gd::GroupedDataFrame) = length(gd.starts)
endof(gd::GroupedDataFrame) = length(gd.starts)
getindex(gd::GroupedDataFrame, idx::Int) = sub(gd.parent, gd.idx[gd.starts[idx]:gd.ends[idx]]) 

function show(io::IO, gd::GroupedDataFrame)
    N = length(gd)
    println(io, "$(typeof(gd))  $N groups with keys: $(gd.cols)")
    println(io, "First Group:")
    show(io, gd[1])
    if N > 1
        println(io, "       :")
        println(io, "       :")
        println(io, "Last Group:")
        show(io, gd[N])
    end
end

##############################################################################
##
## GroupApplied...
##    the result of a split-apply operation
##    TODOs:
##      - better name?
##      - ref
##      - keys, vals
##      - length
##      - start, next, done -- should this return (k,v) or just v?
##      - make it a real associative type? Is there a need to look up key columns?
##
##############################################################################

type GroupApplied
    keys
    vals
end


#
# Apply / map
#

# map() sweeps along groups
function map(f::Function, gd::GroupedDataFrame)
    ## [d[1,gd.cols] => f(d) for d in gd]
    ## [f(g) for g in gd]
    keys = [d[1,gd.cols] for d in gd]
    vals = {f(d) for d in gd}
    GroupApplied(keys,vals)
end

with(x::GroupApplied, e::Expr) = GroupApplied(x.keys, map(with(e), x.vals))
map(f::Function, x::GroupApplied) = GroupApplied(x.keys, map(f, x.vals))



## function map(f::Function, gd::GroupedDataFrame)
##     # preallocate based on the results on the first one
##     x = f(gd[1])
##     res = Array(typeof(x), length(gd))
##     res[1] = x
##     for idx in 2:length(gd)
##         res[idx] = f(gd[idx])
##     end
##     res
## end

# with() sweeps along groups and applies with to each group
function with(gd::GroupedDataFrame, e::Expr)
    keys = [d[1,gd.cols] for d in gd]
    vals = {with(d, e) for d in gd}
    GroupApplied(keys,vals)
end

function combine(x)   # expecting (keys,vals) with keys to be DataFrames and values are what are to be combined
    keys = copy(x.keys)
    vals = map(DataFrame, x.vals)
    for i in 1:length(keys)
        keys[i] = rbind(fill(copy(keys[i]), nrow(vals[i])))
    end
    cbind(rbind(keys), rbind(vals))
end


# within() sweeps along groups and applies within to each group
function within!(gd::GroupedDataFrame, e::Expr)   
    x = [within!(d[:,:], e) for d in gd]
    rbind(x...)
end

within!(x::SubDataFrame, e::Expr) = within!(x[:,:], e)

function within(gd::GroupedDataFrame, e::Expr)  
    x = [within(d, e) for d in gd]
    rbind(x...)
end

within(x::SubDataFrame, e::Expr) = within(x[:,:], e)

# based_on() sweeps along groups and applies based_on to each group
function based_on(gd::GroupedDataFrame, ex::Expr)  
    f = based_on_f(gd.parent, ex)
    x = {f(d) for d in gd}
    idx = rep([1:length(x)], convert(Vector{Int}, map(nrow, x)))
    keydf = gd.parent[gd.idx[gd.starts[idx]], gd.cols]
    resdf = rbind(x)
    cbind(keydf, resdf)
end

function based_on(gd::GroupedDataFrame, f::Function)
    x = {DataFrame(f(d)) for d in gd}
    idx = rep([1:length(x)], convert(Vector{Int}, map(nrow, x)))
    keydf = gd.parent[gd.idx[gd.starts[idx]], gd.cols]
    resdf = rbind(x)
    cbind(keydf, resdf)
end


# default pipelines:
map(f::Function, x::SubDataFrame) = f(x)
(|)(x::GroupedDataFrame, e::Expr) = based_on(x, e)   
(|)(x::GroupApplied, e::Expr) = with(x, e)   
## (|)(x::GroupedDataFrame, f::Function) = map(f, x)

# apply a function to each column in a DataFrame
colwise(f::Function, d::AbstractDataFrame) = {[f(d[idx])] for idx in 1:ncol(d)}
colwise(f::Function, d::GroupedDataFrame) = map(colwise(f), d)
colwise(f::Function) = x -> colwise(f, x)
colwise(f) = x -> colwise(f, x)
# apply several functions to each column in a DataFrame
colwise(fns::Vector{Function}, d::AbstractDataFrame) = [f(d[idx]) for f in fns, idx in 1:ncol(d)][:]
colwise(fns::Vector{Function}, d::GroupedDataFrame) = map(colwise(fns), d)
colwise(fns::Vector{Function}, d::GroupedDataFrame, cn::Vector{String}) = map(colwise(fns), d)
colwise(fns::Vector{Function}) = x -> colwise(fns, x)

function colwise(d::AbstractDataFrame, s::Vector{Symbol}, cn::Vector)
    header = [s2 * "_" * string(s1) for s1 in s, s2 in cn][:]
    payload = colwise(map(eval, s), d)
    df = DataFrame()
    # TODO fix this to assign the longest column first or preallocate
    # based on the maximum length.
    for i in 1:length(header)
        df[header[i]] = payload[i]
    end
    df
end
## function colwise(d::AbstractDataFrame, s::Vector{Symbol}, cn::Vector)
##     header = [s2 * "_" * string(s1) for s1 in s, s2 in cn][:]
##     payload = colwise(map(eval, s), d)
##     DataFrame(payload, header)
## end
colwise(d::AbstractDataFrame, s::Symbol, x) = colwise(d, [s], x)
colwise(d::AbstractDataFrame, s::Vector{Symbol}, x::String) = colwise(d, s, [x])
colwise(d::AbstractDataFrame, s::Symbol) = colwise(d, [s], colnames(d))
colwise(d::AbstractDataFrame, s::Vector{Symbol}) = colwise(d, s, colnames(d))

# TODO make this faster by applying the header just once.
# BUG zero-rowed groupings cause problems here, because a sum of a zero-length
# DataVector is 0 (not 0.0).
function colwise(gd::GroupedDataFrame, s::Vector{Symbol})
    x = map(x -> colwise(without(x, gd.cols),s), gd)
    cbind(rbind(x.keys), rbind(x.vals))
end
colwise(d::GroupedDataFrame, s::Symbol, x) = colwise(d, [s], x)
colwise(d::GroupedDataFrame, s::Vector{Symbol}, x::String) = colwise(d, s, [x])
colwise(d::GroupedDataFrame, s::Symbol) = colwise(d, [s])
(|)(d::GroupedDataFrame, s::Vector{Symbol}) = colwise(d, s)
(|)(d::GroupedDataFrame, s::Symbol) = colwise(d, [s])
colnames(d::GroupedDataFrame) = colnames(d.parent)

# by() convenience function
by(d::AbstractDataFrame, cols, f::Function) = based_on(groupby(d, cols), f)
by(d::AbstractDataFrame, cols, e::Expr) = based_on(groupby(d, cols), e)
by(d::AbstractDataFrame, cols, s::Vector{Symbol}) = colwise(groupby(d, cols), s)
by(d::AbstractDataFrame, cols, s::Symbol) = colwise(groupby(d, cols), s)
function by(d, x::Union(Function,Expr)...)
    res = d
    for e in x
        if isa(e, Function)
            res = e(res)
        else
            res = with(res, e)
        end
    end
    res
end
