# in this file we use cs and cs_i variable names that mean "target columns specification"

# this constant defines which types of values returned by aggregation function
# in combine are considered to produce multiple columns in the resulting data frame
const MULTI_COLS_TYPE = Union{AbstractDataFrame, NamedTuple, DataFrameRow, AbstractMatrix}

# use a constant Vector{Int} as a sentinel to signal that idx_agg has not been computed yet
# we do not use nothing to avoid excessive specialization
const NOTHING_IDX_AGG = Int[]

function gen_groups(idx::Vector{Int})
    groups = zeros(Int, length(idx))
    groups[1] = 1
    j = 1
    last_idx = idx[1]
    @inbounds for i in 2:length(idx)
        cur_idx = idx[i]
        j += cur_idx != last_idx
        last_idx = cur_idx
        groups[i] = j
    end
    return groups
end

function _combine_prepare(gd::GroupedDataFrame,
                          (cs,)::Ref{Any};
                          keepkeys::Bool, ungroup::Bool, copycols::Bool,
                          keeprows::Bool, renamecols::Bool)
    for cei in cs
        if !(cei isa AbstractMatrix && isempty(cei))
            @assert cei isa Union{Pair, Base.Callable, ColumnIndex,
                                  MultiColumnIndex,
                                  AbstractVecOrMat{<:Pair}}
        end
    end
    if !ungroup && !keepkeys
        throw(ArgumentError("keepkeys=false when ungroup=false is not allowed"))
    end

    cs_vec = []
    for p in cs
        if p === nrow
            push!(cs_vec, nrow => :nrow)
        elseif p isa AbstractVecOrMat{<:Pair}
            append!(cs_vec, p)
        else !(p isa AbstractMatrix && isempty(p))
            push!(cs_vec, p)
        end
    end
    return _combine_prepare_norm(gd, cs_vec, keepkeys, ungroup, copycols,
                                 keeprows, renamecols)
end

function _combine_prepare_norm(gd::GroupedDataFrame,
                               cs_vec::Vector{Any},
                               keepkeys::Bool, ungroup::Bool, copycols::Bool,
                               keeprows::Bool, renamecols::Bool)
    if any(x -> x isa Pair && first(x) isa Tuple, cs_vec)
        x = cs_vec[findfirst(x -> first(x) isa Tuple, cs_vec)]
        # an explicit error is thrown as this was allowed in the past
        throw(ArgumentError("passing a Tuple $(first(x)) as column selector is not " *
                            "supported, use a vector $(collect(first(x))) instead"))
    end

    cs_norm = []
    optional_transform = Bool[]
    for c in cs_vec
        arg = normalize_selection(index(parent(gd)), make_pair_concrete(c), renamecols)
        if arg isa AbstractVector{Int}
            for col_idx in arg
                push!(cs_norm, col_idx => identity => _names(gd)[col_idx])
                push!(optional_transform, true)
            end
        else
            push!(cs_norm, arg)
            push!(optional_transform, false)
        end
    end

    # cs_norm holds now either src => fun => dst or just fun
    # if optional_transform[i] is true then the transformation will be skipped
    # if earlier column with a column with the same name was created

    idx, valscat = _combine(gd, cs_norm, optional_transform, copycols, keeprows, renamecols)

    !keepkeys && ungroup && return valscat

    gd_keys = groupcols(gd)
    for key in gd_keys
        if hasproperty(valscat, key)
            if (keeprows && !isequal(valscat[!, key], parent(gd)[!, key])) ||
                (!keeprows && !isequal(valscat[!, key], view(parent(gd)[!, key], idx)))
                throw(ArgumentError("column :$key in returned data frame " *
                                    "is not equal to grouping key :$key"))
            end
        end
    end
    if keeprows
        newparent = select(parent(gd), gd.cols, copycols=copycols)
    else
        newparent = length(gd) > 0 ? parent(gd)[idx, gd.cols] : parent(gd)[1:0, gd.cols]
    end
    added_cols = select(valscat, Not(intersect(gd_keys, _names(valscat))), copycols=false)
    hcat!(newparent, length(gd) > 0 ? added_cols : similar(added_cols, 0), copycols=false)
    ungroup && return newparent

    if length(idx) == 0 && !(keeprows && length(gd_keys) > 0)
        @assert nrow(newparent) == 0
        return GroupedDataFrame(newparent, copy(gd.cols), Int[],
                                Int[], Int[], Int[], 0, Dict{Any, Int}(),
                                Threads.ReentrantLock())
    elseif keeprows
        @assert length(gd_keys) > 0 || idx == gd.idx
        # in this case we are sure that the result GroupedDataFrame has the
        # same structure as the source except that grouping columns are at the start
        return Threads.lock(gd.lazy_lock) do
            return GroupedDataFrame(newparent, copy(gd.cols), gd.groups,
                                    getfield(gd, :idx), getfield(gd, :starts),
                                    getfield(gd, :ends), gd.ngroups,
                                    getfield(gd, :keymap), Threads.ReentrantLock())
        end
    else
        groups = gen_groups(idx)
        @assert groups[end] <= length(gd)
        return GroupedDataFrame(newparent, copy(gd.cols), groups,
                                nothing, nothing, nothing, groups[end], nothing,
                                Threads.ReentrantLock())
    end
end

# Find first value matching condition for each group
# Optimized for situations where a matching value is typically encountered
# among the first rows for each group
function fillfirst!(condf, outcol::AbstractVector, incol::AbstractVector,
                    gd::GroupedDataFrame; rev::Bool=false)
    ngroups = gd.ngroups
    # Use group indices if they have already been computed
    idx = getfield(gd, :idx)
    if idx !== nothing && condf === nothing
        v = rev ? gd.ends : gd.starts
        @inbounds for i in 1:ngroups
            outcol[i] = incol[idx[v[i]]]
        end
    elseif idx !== nothing
        nfilled = 0
        starts = gd.starts
        @inbounds for i in eachindex(outcol)
            s = starts[i]
            offsets = rev ? (nrow(gd[i])-1:-1:0) : (0:nrow(gd[i])-1)
            for j in offsets
                x = incol[idx[s+j]]
                if !condf === nothing || condf(x)
                    outcol[i] = x
                    nfilled += 1
                    break
                end
            end
        end
        if nfilled < length(outcol)
            throw(ArgumentError("some groups contain only missing values"))
        end
    else # Finding first row is faster than computing all group indices
        groups = gd.groups
        if rev
            r = length(groups):-1:1
        else
            r = 1:length(groups)
        end
        filled = fill(false, ngroups)
        nfilled = 0
        @inbounds for i in r
            gix = groups[i]
            x = incol[i]
            if gix > 0 && (condf === nothing || condf(x)) && !filled[gix]
                filled[gix] = true
                outcol[gix] = x
                nfilled += 1
                nfilled == ngroups && break
            end
        end
        if nfilled < length(outcol)
            throw(ArgumentError("some groups contain only missing values"))
        end
    end
    outcol
end

function _agg2idx_map_helper(idx::Vector{Int}, idx_agg::Vector{Int})
    agg2idx_map = fill(-1, length(idx))
    aggj = 1
    @inbounds for (j, idxj) in enumerate(idx)
        while idx_agg[aggj] != idxj
            aggj += 1
            @assert aggj <= length(idx_agg)
        end
        agg2idx_map[j] = aggj
    end
    return agg2idx_map
end

struct TransformationResult
    col_idx::Vector{Int} # index for a column
    col::AbstractVector # computed value of a column
    name::Symbol # name of a column
    optional::Bool # whether a column is allowed to be replaced in the future
end

# the transformation is an aggregation for which we have the fast path
function _combine_process_agg((cs_i,)::Ref{Any},
                              optional_i::Bool,
                              parentdf::AbstractDataFrame,
                              gd::GroupedDataFrame,
                              seen_cols::Dict{Symbol, Tuple{Bool, Int}},
                              trans_res::Vector{TransformationResult},
                              idx_agg::Vector{Int})
    @assert cs_i isa Pair{Int, <:Pair{<:Function, Symbol}}
    @assert isagg(cs_i, gd)
    @assert !optional_i
    out_col_name = last(last(cs_i))
    incol = parentdf[!, first(cs_i)]
    agg = check_aggregate(first(last(cs_i)), incol)
    outcol = agg(incol, gd)

    return function()
        if haskey(seen_cols, out_col_name)
            optional, loc = seen_cols[out_col_name]
            # we have seen this col but it is not allowed to replace it
            optional || throw(ArgumentError("duplicate output column name: :$out_col_name"))
            @assert trans_res[loc].optional && trans_res[loc].name == out_col_name
            trans_res[loc] = TransformationResult(idx_agg, outcol, out_col_name, optional_i)
            seen_cols[out_col_name] = (optional_i, loc)
        else
            push!(trans_res, TransformationResult(idx_agg, outcol, out_col_name, optional_i))
            seen_cols[out_col_name] = (optional_i, length(trans_res))
        end
    end
end

# move one column without transorming it
function _combine_process_noop(cs_i::Pair{<:Union{Int, AbstractVector{Int}}, Pair{typeof(identity), Symbol}},
                               optional_i::Bool,
                               parentdf::AbstractDataFrame,
                               seen_cols::Dict{Symbol, Tuple{Bool, Int}},
                               trans_res::Vector{TransformationResult},
                               idx_keeprows::AbstractVector{Int},
                               copycols::Bool)
    source_cols = first(cs_i)
    out_col_name = last(last(cs_i))
    if length(source_cols) != 1
        throw(ArgumentError("Exactly one column can be transformed to one output column " *
                            "when using identity transformation"))
    end
    outcol = parentdf[!, first(source_cols)]

    return function()
        if haskey(seen_cols, out_col_name)
            optional, loc = seen_cols[out_col_name]
            @assert trans_res[loc].name == out_col_name
            if optional
                if !optional_i
                    @assert trans_res[loc].optional
                    trans_res[loc] = TransformationResult(idx_keeprows, copycols ? copy(outcol) : outcol,
                                                        out_col_name, optional_i)
                    seen_cols[out_col_name] = (optional_i, loc)
                end
            else
                # if optional_i is true, then we ignore processing this column
                optional_i || throw(ArgumentError("duplicate output column name: :$out_col_name"))
            end
        else
            push!(trans_res, TransformationResult(idx_keeprows, copycols ? copy(outcol) : outcol,
                                                out_col_name, optional_i))
            seen_cols[out_col_name] = (optional_i, length(trans_res))
        end
    end
end

# perform a transformation taking SubDataFrame as an input
function _combine_process_callable(wcs_i::Ref{Any},
                                   optional_i::Bool,
                                   parentdf::AbstractDataFrame,
                                   gd::GroupedDataFrame,
                                   seen_cols::Dict{Symbol, Tuple{Bool, Int}},
                                   trans_res::Vector{TransformationResult},
                                   idx_agg::Ref{Vector{Int}})
    cs_i = only(wcs_i)
    @assert cs_i isa Base.Callable
    firstres = length(gd) > 0 ? cs_i(gd[1]) : cs_i(similar(parentdf, 0))
    idx, outcols, nms = _combine_multicol(Ref{Any}(firstres), wcs_i, gd, Ref{Any}(nothing))

    if !(firstres isa Union{AbstractVecOrMat, AbstractDataFrame,
                            NamedTuple{<:Any, <:Tuple{Vararg{AbstractVector}}}})
        lock(gd.lazy_lock) do
            # if idx_agg was not computed yet it is NOTHING_IDX_AGG
            # in this case if we are not passed a vector compute it.
            if idx_agg[] === NOTHING_IDX_AGG
                idx_agg[] = Vector{Int}(undef, length(gd))
                fillfirst!(nothing, idx_agg[], 1:length(gd.groups), gd)
            end
            @assert idx == idx_agg[]
            idx = idx_agg[]
        end
    end
    @assert length(outcols) == length(nms)
    return function()
        for j in eachindex(outcols)
            outcol = outcols[j]
            out_col_name = nms[j]
            if haskey(seen_cols, out_col_name)
                optional, loc = seen_cols[out_col_name]
                # if column was seen and it is optional now ignore it
                if !optional_i
                    optional, loc = seen_cols[out_col_name]
                    # we have seen this col but it is not allowed to replace it
                    optional || throw(ArgumentError("duplicate output column name: :$out_col_name"))
                    @assert trans_res[loc].optional && trans_res[loc].name == out_col_name
                    trans_res[loc] = TransformationResult(idx, outcol, out_col_name, optional_i)
                    seen_cols[out_col_name] = (optional_i, loc)
                end
            else
                push!(trans_res, TransformationResult(idx, outcol, out_col_name, optional_i))
                seen_cols[out_col_name] = (optional_i, length(trans_res))
            end
        end
    end
end

# perform a transformation specified using the Pair notation with a single output column
function _combine_process_pair_symbol(optional_i::Bool,
                                      gd::GroupedDataFrame,
                                      seen_cols::Dict{Symbol, Tuple{Bool, Int}},
                                      trans_res::Vector{TransformationResult},
                                      idx_agg::Ref{Vector{Int}},
                                      out_col_name::Symbol,
                                      firstmulticol::Bool,
                                      (firstres,)::Ref{Any},
                                      wfun::Ref{Any},
                                      wincols::Ref{Any})
    @assert only(wfun) isa Base.Callable
    @assert only(wincols) isa Union{Tuple, NamedTuple}

    if firstmulticol
        throw(ArgumentError("a single value or vector result is required (got $(typeof(firstres)))"))
    end
    # if idx_agg was not computed yet it is NOTHING_IDX_AGG
    # in this case if we are not passed a vector compute it.
    lock(gd.lazy_lock) do
        if !(firstres isa AbstractVector) && idx_agg[] === NOTHING_IDX_AGG
            idx_agg[] = Vector{Int}(undef, length(gd))
            fillfirst!(nothing, idx_agg[], 1:length(gd.groups), gd)
        end
    end
    # TODO: if firstres is a vector we recompute idx for every function
    # this could be avoided - it could be computed only the first time
    # and later we could just check if lengths of groups match this first idx

    # the last argument passed to _combine_with_first informs it about precomputed
    # idx. Currently we do it only for single-row return values otherwise we pass
    # NOTHING_IDX_AGG to signal that idx has to be computed in _combine_with_first
    idx, outcols, _ = _combine_with_first(Ref{Any}(wrap(firstres)), wfun, gd, wincols,
                                          firstmulticol,
                                          firstres isa AbstractVector ? NOTHING_IDX_AGG : idx_agg[])
    @assert length(outcols) == 1
    outcol = outcols[1]

    return function()
        if haskey(seen_cols, out_col_name)
            # if column was seen and it is optional now ignore it
            if !optional_i
                optional, loc = seen_cols[out_col_name]
                # we have seen this col but it is not allowed to replace it
                optional || throw(ArgumentError("duplicate output column name: :$out_col_name"))
                @assert trans_res[loc].optional && trans_res[loc].name == out_col_name
                trans_res[loc] = TransformationResult(idx, outcol, out_col_name, optional_i)
                seen_cols[out_col_name] = (optional_i, loc)
            end
        else
            push!(trans_res, TransformationResult(idx, outcol, out_col_name, optional_i))
            seen_cols[out_col_name] = (optional_i, length(trans_res))
        end
    end
end

# perform a transformation specified using the Pair notation with multiple output columns
function _combine_process_pair_astable(optional_i::Bool,
                                       gd::GroupedDataFrame,
                                       seen_cols::Dict{Symbol, Tuple{Bool, Int}},
                                       trans_res::Vector{TransformationResult},
                                       idx_agg::Ref{Vector{Int}},
                                       out_col_name::Union{Type{AsTable}, AbstractVector{Symbol}},
                                       firstmulticol::Bool,
                                       (firstres,)::Ref{Any},
                                       wfun::Ref{Any},
                                       wincols::Ref{Any})
    fun = only(wfun)
    @assert fun isa Base.Callable
    @assert only(wincols) isa Union{Tuple, NamedTuple}
    if firstres isa AbstractVector
        idx, outcol_vec, _ = _combine_with_first(Ref{Any}(wrap(firstres)), wfun, gd, wincols,
                                                 firstmulticol, NOTHING_IDX_AGG)
        @assert length(outcol_vec) == 1
        res = outcol_vec[1]
        @assert length(res) > 0

        kp1 = keys(res[1])
        prepend = all(x -> x isa Integer, kp1)
        if !(prepend || all(x -> x isa Symbol, kp1) || all(x -> x isa AbstractString, kp1))
            throw(ArgumentError("keys of the returned elements must be " *
                                "`Symbol`s, strings or integers"))
        end
        if any(x -> !isequal(keys(x), kp1), res)
            throw(ArgumentError("keys of the returned elements must be identical"))
        end
        outcols = [[x[n] for x in res] for n in kp1]
        nms = [prepend ? Symbol("x", n) : Symbol(n) for n in kp1]
    else
        if !firstmulticol
            firstres = Tables.columntable(firstres)
            oldfun = fun
            fun = (x...) -> Tables.columntable(oldfun(x...))
        end
        idx, outcols, nms = _combine_multicol(Ref{Any}(firstres), Ref{Any}(fun), gd, wincols)

        if !(firstres isa Union{AbstractVecOrMat, AbstractDataFrame,
            NamedTuple{<:Any, <:Tuple{Vararg{AbstractVector}}}})
            lock(gd.lazy_lock) do
                # if idx_agg was not computed yet it is nothing
                # in this case if we are not passed a vector compute it.
                if idx_agg[] === NOTHING_IDX_AGG
                    idx_agg[] = Vector{Int}(undef, length(gd))
                    fillfirst!(nothing, idx_agg[], 1:length(gd.groups), gd)
                end
                @assert idx == idx_agg[]
                idx = idx_agg[]
            end
        end
        @assert length(outcols) == length(nms)
    end
    if out_col_name isa AbstractVector{Symbol}
        if length(out_col_name) != length(nms)
            throw(ArgumentError("Number of returned columns does not " *
                                "match the length of requested output"))
        else
            nms = out_col_name
        end
    end
    return function()
        for j in eachindex(outcols)
            outcol = outcols[j]
            out_col_name = nms[j]
            if haskey(seen_cols, out_col_name)
                optional, loc = seen_cols[out_col_name]
                # if column was seen and it is optional now ignore it
                if !optional_i
                    optional, loc = seen_cols[out_col_name]
                    # we have seen this col but it is not allowed to replace it
                    optional || throw(ArgumentError("duplicate output column name: :$out_col_name"))
                    @assert trans_res[loc].optional && trans_res[loc].name == out_col_name
                    trans_res[loc] = TransformationResult(idx, outcol, out_col_name, optional_i)
                    seen_cols[out_col_name] = (optional_i, loc)
                end
            else
                push!(trans_res, TransformationResult(idx, outcol, out_col_name, optional_i))
                seen_cols[out_col_name] = (optional_i, length(trans_res))
            end
        end
    end
end

# perform a transformation specified using the Pair notation
# cs_i is a Pair that has many possible forms so this function is used to dispatch
# to an appropriate more specialized function
function _combine_process_pair((cs_i,)::Ref{Any},
                               optional_i::Bool,
                               parentdf::AbstractDataFrame,
                               gd::GroupedDataFrame,
                               seen_cols::Dict{Symbol, Tuple{Bool, Int}},
                               trans_res::Vector{TransformationResult},
                               idx_agg::Ref{Vector{Int}})
    @assert cs_i isa Pair

    source_cols, (fun, out_col_name) = cs_i

    if source_cols isa Int
        incols = (parentdf[!, source_cols],)
    elseif source_cols isa AsTable
        incols = Tables.columntable(select(parentdf,
                                           source_cols.cols,
                                           copycols=false))
    else
        @assert source_cols isa AbstractVector{Int}
        incols = ntuple(i -> parentdf[!, source_cols[i]], length(source_cols))
    end

    firstres = length(gd) > 0 ?
               do_call(fun, gd.idx, gd.starts, gd.ends, gd, incols, 1) :
               do_call(fun, Int[], 1:1, 0:0, gd, incols, 1)
    firstmulticol = firstres isa MULTI_COLS_TYPE

    if out_col_name isa Symbol
        return _combine_process_pair_symbol(optional_i, gd, seen_cols, trans_res, idx_agg,
                                            out_col_name, firstmulticol, Ref{Any}(firstres),
                                            Ref{Any}(fun), Ref{Any}(incols))
    end
    if out_col_name == AsTable || out_col_name isa AbstractVector{Symbol}
        return _combine_process_pair_astable(optional_i, gd, seen_cols, trans_res, idx_agg,
                                             out_col_name, firstmulticol, Ref{Any}(firstres),
                                             Ref{Any}(fun), Ref{Any}(incols))
    end
    throw(ArgumentError("unsupported target column name specifier $out_col_name"))
end

function prepare_idx_keeprows(idx::AbstractVector{<:Integer},
                              starts::AbstractVector{<:Integer},
                              ends::AbstractVector{<:Integer},
                              nrowparent::Integer)
    idx_keeprows = Vector{Int}(undef, nrowparent)
    i = 0
    for (s, e) in zip(starts, ends)
        v = idx[s]
        for k in s:e
            i += 1
            idx_keeprows[i] = v
        end
    end
    @assert i == nrowparent
    return idx_keeprows
end

function _combine(gd::GroupedDataFrame,
                  cs_norm::Vector{Any}, optional_transform::Vector{Bool},
                  copycols::Bool, keeprows::Bool, renamecols::Bool)
    if isempty(cs_norm)
        if keeprows && nrow(parent(gd)) > 0 && minimum(gd.groups) == 0
            throw(ArgumentError("select and transform do not support " *
                                "`GroupedDataFrame`s from which some groups have "*
                                "been dropped (including skipmissing=true)"))
        end
        return Int[], DataFrame()
    end

    if keeprows
        if nrow(parent(gd)) > 0 && minimum(gd.groups) == 0
            throw(ArgumentError("select and transform do not support " *
                                "`GroupedDataFrame`s from which some groups have "*
                                "been dropped (including skipmissing=true)"))
        end
        idx_keeprows = prepare_idx_keeprows(gd.idx, gd.starts, gd.ends, nrow(parent(gd)))
    else
        idx_keeprows = Int[]
    end

    idx_agg = Ref(NOTHING_IDX_AGG)
    if length(gd) > 0 && any(x -> isagg(x, gd), cs_norm)
        # Compute indices of representative rows only once for all AbstractAggregates
        idx_agg[] = Vector{Int}(undef, length(gd))
        fillfirst!(nothing, idx_agg[], 1:length(gd.groups), gd)
    elseif length(gd) == 0 || !all(x -> isagg(x, gd), cs_norm)
        # Trigger computation of indices
        # This can speed up some aggregates that would not trigger this on their own
        @assert gd.idx !== nothing
    end

    trans_res = Vector{TransformationResult}()

    # seen_cols keeps an information about location of columns already processed
    # and if a given column can be replaced in the future
    seen_cols = Dict{Symbol, Tuple{Bool, Int}}()

    tasks = similar(cs_norm, Task)

    parentdf = parent(gd)
    # Operations are run in separate tasks, except two parts:
    # - the first task that needs idx_agg computes it;
    #   a lock ensures others wait for it to complete
    # - once all tasks are done, they need sequential postprocessing
    #   since their order affects that of columns
    for i in eachindex(cs_norm, optional_transform, tasks)
        cs_i = cs_norm[i]
        optional_i = optional_transform[i]

        tasks[i] = @spawn if length(gd) > 0 && isagg(cs_i, gd)
            _combine_process_agg(Ref{Any}(cs_i), optional_i, parentdf, gd, seen_cols, trans_res, idx_agg[])
        elseif keeprows && cs_i isa Pair && first(last(cs_i)) === identity &&
               !(first(cs_i) isa AsTable) && (last(last(cs_i)) isa Symbol)
            # this is a fast path used when we pass a column or rename a column in select or transform
            _combine_process_noop(cs_i, optional_i, parentdf, seen_cols, trans_res, idx_keeprows, copycols)
        elseif cs_i isa Base.Callable
            _combine_process_callable(Ref{Any}(cs_i), optional_i, parentdf, gd, seen_cols, trans_res, idx_agg)
        else
            @assert cs_i isa Pair
            _combine_process_pair(Ref{Any}(cs_i), optional_i, parentdf, gd, seen_cols, trans_res, idx_agg)
        end
    end
    # Workaround JuliaLang/julia#38931:
    # we want to preserve the exception type thrown in user code,
    # and print the backtrace corresponding to it
    for t in tasks
        try
            wait(t)
        catch e
            @static if VERSION > v"1.3"
                if e isa TaskFailedException
                    throw(t.exception)
                else
                    rethrow(e)
                end
            else
                if e isa ErrorException
                    throw(t.exception)
                else
                    rethrow(e)
                end
            end
        end
    end
    # Post-processing has to be run sequentially
    # since the order of operations determines that of columns
    for t in tasks
        postprocessf = fetch(t)
        postprocessf()
    end

    isempty(trans_res) && return Int[], DataFrame()
    # idx_agg[] === NOTHING_IDX_AGG then we have only functions that
    # returned multiple rows and idx_loc = 1
    idx_loc = findfirst(x -> x.col_idx !== idx_agg[], trans_res)
    if !keeprows && isnothing(idx_loc)
        @assert idx_agg[] !== NOTHING_IDX_AGG
        idx = idx_agg[]
    else
        idx = keeprows ? idx_keeprows : trans_res[idx_loc].col_idx
        agg2idx_map = nothing
        for i in 1:length(trans_res)
            if trans_res[i].col_idx !== idx
                if trans_res[i].col_idx === idx_agg[]
                    # we perform pseudo broadcasting here
                    # keep -1 as a sentinel for errors
                    if isnothing(agg2idx_map)
                        agg2idx_map = _agg2idx_map_helper(idx, idx_agg[])
                    end
                    trans_res[i] = TransformationResult(idx_agg[], trans_res[i].col[agg2idx_map],
                                                        trans_res[i].name, trans_res[i].optional)
                elseif idx != trans_res[i].col_idx
                    if keeprows
                        throw(ArgumentError("all functions must return vectors with " *
                                            "as many values as rows in each group"))
                    else
                        throw(ArgumentError("all functions must return vectors of the same length"))
                    end
                end
            end
        end
    end

    # here first field in trans_res[i] is used to keep track how the column was generated
    # a correct index is stored in idx variable

    @sync for i in eachindex(trans_res)
        let i=i
            @spawn reorder_cols!(trans_res, i, trans_res[i].col, trans_res[i].col_idx,
                                 keeprows, idx_keeprows, gd)
        end
    end

    outcols = AbstractVector[x.col for x in trans_res]
    nms = Symbol[x.name for x in trans_res]
    # this check is redundant given we check idx above
    # but it is safer to double check and it is cheap
    @assert all(x -> length(x) == length(outcols[1]), outcols)
    return idx, DataFrame(outcols, nms, copycols=false)
end

function reorder_cols!(trans_res::Vector{TransformationResult}, i::Integer,
                       col::AbstractVector, col_idx::Vector{Int}, keeprows::Bool,
                       idx_keeprows::Vector{Int}, gd::GroupedDataFrame)
    if keeprows && col_idx !== idx_keeprows # we need to reorder the column
        newcol = similar(col)
        # we can probably make it more efficient, but I leave it as an optimization for the future
        gd_idx = gd.idx
        k = 0
        for (s, e) in zip(gd.starts, gd.ends)
            for j in s:e
                k += 1
                @inbounds newcol[gd_idx[j]] = col[k]
            end
        end
        @assert k == length(gd_idx)
        trans_res[i] = TransformationResult(col_idx, newcol, trans_res[i].name, trans_res[i].optional)
    end
end

function combine(@nospecialize(f::Base.Callable), gd::GroupedDataFrame;
                 keepkeys::Bool=true, ungroup::Bool=true, renamecols::Bool=true)
    if f isa Colon
        throw(ArgumentError("First argument must be a transformation if the second argument is a GroupedDataFrame"))
    end
    return combine(gd, f, keepkeys=keepkeys, ungroup=ungroup, renamecols=renamecols)
end

combine(@nospecialize(f::Pair), gd::GroupedDataFrame;
        keepkeys::Bool=true, ungroup::Bool=true, renamecols::Bool=true) =
    throw(ArgumentError("First argument must be a transformation if the second argument is a GroupedDataFrame. " *
                        "You can pass a `Pair` as the second argument of the transformation. If you want the return " *
                        "value to be processed as having multiple columns add `=> AsTable` suffix to the pair."))

combine(gd::GroupedDataFrame,
        @nospecialize(args::Union{Pair, Base.Callable, ColumnIndex, MultiColumnIndex,
                                  AbstractVecOrMat}...);
        keepkeys::Bool=true, ungroup::Bool=true, renamecols::Bool=true) =
    _combine_prepare(gd, Ref{Any}(map(x -> broadcast_pair(parent(gd), x), args)),
                     keepkeys=keepkeys, ungroup=ungroup,
                     copycols=true, keeprows=false, renamecols=renamecols)

function select(@nospecialize(f::Base.Callable), gd::GroupedDataFrame; copycols::Bool=true,
                keepkeys::Bool=true, ungroup::Bool=true, renamecols::Bool=true)
    if f isa Colon
        throw(ArgumentError("First argument must be a transformation if the second argument is a grouped data frame"))
    end
    return select(gd, f, copycols=copycols, keepkeys=keepkeys, ungroup=ungroup)
end

select(gd::GroupedDataFrame, @nospecialize(args::Union{Pair, Base.Callable, ColumnIndex, MultiColumnIndex,
                                                       AbstractVecOrMat}...);
       copycols::Bool=true, keepkeys::Bool=true, ungroup::Bool=true, renamecols::Bool=true) =
    _combine_prepare(gd, Ref{Any}(map(x -> broadcast_pair(parent(gd), x), args)),
                     copycols=copycols, keepkeys=keepkeys,
                     ungroup=ungroup, keeprows=true, renamecols=renamecols)

function transform(@nospecialize(f::Base.Callable), gd::GroupedDataFrame; copycols::Bool=true,
                keepkeys::Bool=true, ungroup::Bool=true, renamecols::Bool=true)
    if f isa Colon
        throw(ArgumentError("First argument must be a transformation if the second argument is a grouped data frame"))
    end
    return transform(gd, f, copycols=copycols, keepkeys=keepkeys, ungroup=ungroup)
end

function transform(gd::GroupedDataFrame, @nospecialize(args::Union{Pair, Base.Callable, ColumnIndex, MultiColumnIndex,
                                                                   AbstractVecOrMat}...);
                   copycols::Bool=true, keepkeys::Bool=true, ungroup::Bool=true, renamecols::Bool=true)
    res = select(gd, :, args..., copycols=copycols, keepkeys=keepkeys,
                 ungroup=ungroup, renamecols=renamecols)
    # res can be a GroupedDataFrame based on DataFrame or a DataFrame,
    # so parent always gives a data frame
    select!(parent(res), propertynames(parent(gd)), :)
    return res
end

function select!(@nospecialize(f::Base.Callable), gd::GroupedDataFrame; ungroup::Bool=true, renamecols::Bool=true)
    if f isa Colon
        throw(ArgumentError("First argument must be a transformation if the second argument is a grouped data frame"))
    end
    return select!(gd, f, ungroup=ungroup)
end

function select!(gd::GroupedDataFrame,
                 @nospecialize(args::Union{Pair, Base.Callable, ColumnIndex, MultiColumnIndex,
                                           AbstractVecOrMat}...);
                 ungroup::Bool=true, renamecols::Bool=true)
    df = parent(gd)
    if df isa DataFrame
        newdf = select(gd, args..., copycols=false, renamecols=renamecols)
    else
        @assert df isa SubDataFrame
        newdf = select(gd, args..., copycols=true, renamecols=renamecols)
    end
    _replace_columns!(df, newdf)
    return ungroup ? df : gd
end

function transform!(@nospecialize(f::Base.Callable), gd::GroupedDataFrame; ungroup::Bool=true, renamecols::Bool=true)
    if f isa Colon
        throw(ArgumentError("First argument must be a transformation if the second argument is a grouped data frame"))
    end
    return transform!(gd, f, ungroup=ungroup)
end

function transform!(gd::GroupedDataFrame,
                    @nospecialize(args::Union{Pair, Base.Callable, ColumnIndex, MultiColumnIndex,
                                              AbstractVecOrMat}...);
                    ungroup::Bool=true, renamecols::Bool=true)
    df = parent(gd)
    if df isa DataFrame
        newdf = select(gd, :, args..., copycols=false, renamecols=renamecols)
    else
        @assert df isa SubDataFrame
        newdf = select(gd, :, args..., copycols=true, renamecols=renamecols)
    end
    select!(newdf, propertynames(df), :)
    _replace_columns!(df, newdf)
    return ungroup ? df : gd
end
