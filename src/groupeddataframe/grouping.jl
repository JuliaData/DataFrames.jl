#
#  Split - Apply - Combine operations
#

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
function groupby{T}(d::AbstractDataFrame, cols::Vector{T})
    ## a subset of Wes McKinney's algorithm here:
    ##     http://wesmckinney.com/blog/?p=489

    ncols = length(cols)
    # use the pool trick to get a set of integer references for each unique item
    dv = PooledDataArray(d[cols[ncols]])
    # if there are NAs, add 1 to the refs to avoid underflows in x later
    dv_has_nas = (findfirst(dv.refs, 0) > 0 ? 1 : 0)
    x = copy(dv.refs) .+ dv_has_nas
    # also compute the number of groups, which is the product of the set lengths
    ngroups = length(dv.pool) + dv_has_nas
    # if there's more than 1 column, do roughly the same thing repeatedly
    for j = (ncols - 1):-1:1
        dv = PooledDataArray(d[cols[j]])
        dv_has_nas = (findfirst(dv.refs, 0) > 0 ? 1 : 0)
        for i = 1:nrow(d)
            x[i] += (dv.refs[i] + dv_has_nas- 1) * ngroups
        end
        ngroups = ngroups * (length(dv.pool) + dv_has_nas)
        # TODO if ngroups is really big, shrink it
    end
    (idx, starts) = DataArrays.groupsort_indexer(x, ngroups)
    # Remove zero-length groupings
    starts = _uniqueofsorted(starts)
    ends = [starts[2:end] .- 1]
    GroupedDataFrame(d, cols, idx, starts[1:end-1], ends)
end
groupby(d::AbstractDataFrame, cols) = groupby(d, [cols])

# add a function curry
groupby{T}(cols::Vector{T}) = x -> groupby(x, cols)
groupby(cols) = x -> groupby(x, cols)

Base.start(gd::GroupedDataFrame) = 1
Base.next(gd::GroupedDataFrame, state::Int) =
    (sub(gd.parent, gd.idx[gd.starts[state]:gd.ends[state]]),
     state + 1)
Base.done(gd::GroupedDataFrame, state::Int) = state > length(gd.starts)
Base.length(gd::GroupedDataFrame) = length(gd.starts)
Base.endof(gd::GroupedDataFrame) = length(gd.starts)
Base.first(gd::GroupedDataFrame) = gd[1]
Base.last(gd::GroupedDataFrame) = gd[end]

Base.getindex(gd::GroupedDataFrame, idx::Int) =
    sub(gd.parent, gd.idx[gd.starts[idx]:gd.ends[idx]])
Base.getindex(gd::GroupedDataFrame, I::AbstractArray{Bool}) =
    GroupedDataFrame(gd.parent, gd.cols, gd.idx, gd.starts[I], gd.ends[I])

Base.names(gd::GroupedDataFrame) = names(gd.parent)

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
    keys::Vector{AbstractDataFrame}
    vals::Vector

    function GroupApplied(keys, vals)
        if length(keys) != length(vals)
            error("GroupApplied requires keys and vals be of equal length.")
        end
        new(keys, vals)
    end
end


#
# Apply / map
#

# map() sweeps along groups
function Base.map(f::Function, gd::GroupedDataFrame)
    ## [d[1,gd.cols] => f(d) for d in gd]
    ## [f(g) for g in gd]
    keys = [d[1, gd.cols] for d in gd]
    vals = Any[f(d) for d in gd]
    GroupApplied(keys, vals)
end

Base.map(f::Function, x::GroupApplied) = GroupApplied(x.keys, map(f, x.vals))

function combine(x::GroupApplied)
    vals = convert(Vector{DataFrame}, x.vals)
    rows = sum(nrow, vals)
    ret = similar(x.keys[1], rows)
    for j in 1:length(ret)
        i = 1
        col = ret[j]
        for idx in 1:length(x.keys)
            val = x.keys[idx][j][1]
            @inbounds for _ = 1:size(vals[idx], 1)
                col[i] = val
                i += 1
            end
        end
    end
    hcat!(ret, vcat(vals))
end

wrap(df::AbstractDataFrame) = df
wrap(A::Matrix) = convert(DataFrame, A)
wrap(s::Any) = DataFrame(x1 = s)

function based_on(gd::GroupedDataFrame, f::Function)
    x = AbstractDataFrame[wrap(f(d)) for d in gd]
    idx = rep(1:length(x), map(nrow, x))
    keydf = gd.parent[gd.idx[gd.starts[idx]], gd.cols]
    resdf = vcat(x)
    hcat!(keydf, resdf)
end


# apply a function to each column in a DataFrame
colwise(f::Function, d::AbstractDataFrame) = Any[[f(d[idx])] for idx in 1:size(d, 2)]
colwise(f::Function, gd::GroupedDataFrame) = map(colwise(f), gd)
colwise(f::Function) = x -> colwise(f, x)
colwise(f) = x -> colwise(f, x)
# apply several functions to each column in a DataFrame
colwise(fns::Vector{Function}, d::AbstractDataFrame) = Any[[f(d[idx])] for f in fns, idx in 1:size(d, 2)][:]
colwise(fns::Vector{Function}, gd::GroupedDataFrame) = map(colwise(fns), gd)
# TODO: ignoring `cn`???
colwise(fns::Vector{Function}, gd::GroupedDataFrame, cn::Vector{String}) = map(colwise(fns), gd)
colwise(fns::Vector{Function}) = x -> colwise(fns, x)

# By convenience functions
by(d::AbstractDataFrame, cols, f::Function) = based_on(groupby(d, cols), f)
by(f::Function, d::AbstractDataFrame, cols) = by(d, cols, f)


aggregate(d::Union(AbstractDataFrame, GroupedDataFrame), fs, cn::Symbol) = aggregate(d, fs, [cn])
aggregate(d::Union(AbstractDataFrame, GroupedDataFrame), fs::Function, cn) = aggregate(d, [fs], cn)

# Applies a set of functions over a DataFrame, in the from of a cross-product
aggregate(d::AbstractDataFrame, fs) = aggregate(d, fs, names(d))
function aggregate(d::AbstractDataFrame, fs::Vector{Function}, cn::Vector{Symbol})
    if length(cn) != length(d)
        error("Length of 'cn' must match columns of 'd.'")
    end
    headers = _makeheaders(fs, cn)
    _aggregate(d, fs, headers)
end

# Applies aggregate to non-key cols of each SubDataFrame of a GroupedDataFrame
Base.(:|>)(gd::GroupedDataFrame, fs::Vector{Function}) = aggregate(gd, fs)
Base.(:|>)(gd::GroupedDataFrame, fs::Function) = aggregate(gd, [fs])
aggregate(gd::GroupedDataFrame, fs) = aggregate(gd, fs, _setdiff(names(gd), gd.cols))
function aggregate(gd::GroupedDataFrame, fs::Vector{Function}, cn::Vector{Symbol})
    if length(cn) != length(gd) - length(gd.cols)
        error("Length of 'cn' must match non-key columns of 'gd.'")
    end
    headers = _makeheaders(fs, cn)
    combine(map(x -> _aggregate(without(x, gd.cols), fs, headers), gd))
end

# Groups DataFrame by cols before applying aggregate
function aggregate{T <: ColumnIndex}(d::AbstractDataFrame,
                                     cols::Union(T, AbstractVector{T}),
                                     fs::Union(Function, Vector{Function}))
    aggregate(groupby(d, cols), fs)
end


function _makeheaders(fs::Vector{Function}, cn::Vector{Symbol})
    fnames = _fnames(fs) # see other/utils.jl
    scn = [string(x) for x in cn]
    [symbol("$(colname)_$(fname)") for fname in fnames, colname in scn][:]
end

function _aggregate(d::AbstractDataFrame, fs::Vector{Function}, headers::Vector{Symbol})
    DataFrame(colwise(fs, d), headers)
end
