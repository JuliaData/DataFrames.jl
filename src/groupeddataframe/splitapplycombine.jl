# in this file we use cs and cs_i variable names that mean "target columns specification"

# this constant defines which types of values returned by aggregation function
# in combine are considered to produce multiple columns in the resulting data frame
const MULTI_COLS_TYPE = Union{AbstractDataFrame, NamedTuple, DataFrameRow,
                              Tables.AbstractRow, AbstractMatrix}

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
                          keeprows::Bool, renamecols::Bool,
                          threads::Bool)
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
                                 keeprows, renamecols, threads)
end

function _combine_prepare_norm(gd::GroupedDataFrame,
                               cs_vec::Vector{Any},
                               keepkeys::Bool, ungroup::Bool, copycols::Bool,
                               keeprows::Bool, renamecols::Bool,
                               threads::Bool)
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

    idx, valscat, metacols = _combine(gd, cs_norm, optional_transform, copycols,
                                      keeprows, renamecols, threads)

    pgd = parent(gd)
    _copy_table_note_metadata!(valscat, pgd)
    @assert ncol(valscat) == length(metacols)
    for (out_col_idx, in_col_idx) in enumerate(metacols)
        in_col_idx > 0 && _copy_col_note_metadata!(valscat, out_col_idx, pgd, in_col_idx)
    end

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
# rev keyword argument should be only used to signal that we process the `last` function
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
    return outcol
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
    metadatacol::Int # whether metadata should be propagated:
                     # 0 if not
                     # if greater than zero column number of metadata source
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
    in_col_idx = first(cs_i)
    incol = parentdf[!, in_col_idx]
    agg = check_aggregate(first(last(cs_i)), incol)
    outcol = agg(incol, gd)

    return function()
        if haskey(seen_cols, out_col_name)
            optional, loc = seen_cols[out_col_name]
            # we have seen this col but it is not allowed to replace it
            optional || throw(ArgumentError("duplicate output column name: :$out_col_name"))
            @assert trans_res[loc].optional && trans_res[loc].name == out_col_name
            trans_res[loc] = TransformationResult(idx_agg, outcol, out_col_name, optional_i,
                                                  columnindex(parentdf, out_col_name) == in_col_idx ? in_col_idx : 0)
            seen_cols[out_col_name] = (optional_i, loc)
        else
            push!(trans_res, TransformationResult(idx_agg, outcol, out_col_name, optional_i,
                                                  columnindex(parentdf, out_col_name) == in_col_idx ? in_col_idx : 0))
            seen_cols[out_col_name] = (optional_i, length(trans_res))
        end
    end
end

function _combine_process_groupindices((cs_i,)::Ref{Any},
                                       optional_i::Bool,
                                       parentdf::AbstractDataFrame,
                                       gd::GroupedDataFrame,
                                       seen_cols::Dict{Symbol, Tuple{Bool, Int}},
                                       trans_res::Vector{TransformationResult},
                                       idx_agg::Vector{Int})
    @assert cs_i isa Pair{Vector{Int}, Pair{typeof(groupindices), Symbol}}
    @assert first(cs_i) isa Vector{Int} && isempty(first(cs_i))
    @assert !optional_i
    @assert idx_agg !== NOTHING_IDX_AGG
    out_col_name = last(last(cs_i))
    outcol = 1:length(gd)

    return function()
        if haskey(seen_cols, out_col_name)
            optional, loc = seen_cols[out_col_name]
            # we have seen this col but it is not allowed to replace it
            optional || throw(ArgumentError("duplicate output column name: :$out_col_name"))
            @assert trans_res[loc].optional && trans_res[loc].name == out_col_name
            trans_res[loc] = TransformationResult(idx_agg, outcol, out_col_name, optional_i, 0)
            seen_cols[out_col_name] = (optional_i, loc)
        else
            push!(trans_res, TransformationResult(idx_agg, outcol, out_col_name, optional_i, 0))
            seen_cols[out_col_name] = (optional_i, length(trans_res))
        end
    end
end

function _combine_process_proprow((cs_i,)::Ref{Any},
                                  optional_i::Bool,
                                  parentdf::AbstractDataFrame,
                                  gd::GroupedDataFrame,
                                  seen_cols::Dict{Symbol, Tuple{Bool, Int}},
                                  trans_res::Vector{TransformationResult},
                                  idx_agg::Vector{Int})
    @assert cs_i isa Pair{Vector{Int}, Pair{typeof(proprow), Symbol}}
    @assert first(cs_i) isa Vector{Int} && isempty(first(cs_i))
    @assert !optional_i
    @assert idx_agg !== NOTHING_IDX_AGG
    out_col_name = last(last(cs_i))

    # introduce outcol1 and outcol2 as without it outcol is boxed
    # since it is later used inside the anonymous function we return
    if getfield(gd, :idx) === nothing
        outcol1 = zeros(Float64, length(gd) + 1)
        @inbounds @simd for gix in gd.groups
            outcol1[gix + 1] += 1
        end
        popfirst!(outcol1)
        outcol1 ./= sum(outcol1)
        outcol = outcol1
    else
        outcol2 = Vector{Float64}(undef, length(gd))
        outcol2 .= gd.ends .- gd.starts .+ 1
        outcol2 ./= sum(outcol2)
        outcol = outcol2
    end

    return function()
        if haskey(seen_cols, out_col_name)
            optional, loc = seen_cols[out_col_name]
            # we have seen this col but it is not allowed to replace it
            optional || throw(ArgumentError("duplicate output column name: :$out_col_name"))
            @assert trans_res[loc].optional && trans_res[loc].name == out_col_name
            trans_res[loc] = TransformationResult(idx_agg, outcol, out_col_name, optional_i, 0)
            seen_cols[out_col_name] = (optional_i, loc)
        else
            push!(trans_res, TransformationResult(idx_agg, outcol, out_col_name, optional_i, 0))
            seen_cols[out_col_name] = (optional_i, length(trans_res))
        end
    end
end

# move one column without transforming it
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
                                                          out_col_name, optional_i, only(source_cols))
                    seen_cols[out_col_name] = (optional_i, loc)
                end
            else
                # if optional_i is true, then we ignore processing this column
                optional_i || throw(ArgumentError("duplicate output column name: :$out_col_name"))
            end
        else
            push!(trans_res, TransformationResult(idx_keeprows, copycols ? copy(outcol) : outcol,
                                                  out_col_name, optional_i, only(source_cols)))
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
                                   idx_agg::Ref{Vector{Int}},
                                   threads::Bool)
    cs_i = only(wcs_i)
    @assert cs_i isa Base.Callable
    firstres = length(gd) > 0 ? cs_i(gd[1]) : cs_i(similar(parentdf, 0))
    idx, outcols, nms = _combine_multicol(Ref{Any}(firstres), wcs_i, gd,
                                          Ref{Any}(nothing), threads)

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
                    trans_res[loc] = TransformationResult(idx, outcol, out_col_name, optional_i, 0)
                    seen_cols[out_col_name] = (optional_i, loc)
                end
            else
                push!(trans_res, TransformationResult(idx, outcol, out_col_name, optional_i, 0))
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
                                      wincols::Ref{Any},
                                      threads::Bool,
                                      (source_cols,)::Ref{Any})
    @assert only(wfun) isa Base.Callable
    @assert only(wincols) isa Union{Tuple, NamedTuple}

    if firstmulticol
        throw(ArgumentError("a single value or vector result is required " *
                            "(got $(typeof(firstres))). Maybe you " *
                            "forgot to wrap the return value of " *
                            "the operation with `Ref`?"))
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
                                          firstres isa AbstractVector ? NOTHING_IDX_AGG : idx_agg[],
                                          threads)
    @assert length(outcols) == 1
    outcol = outcols[1]

    if (source_cols isa Int ||
        (source_cols isa AbstractVector{Int} && length(source_cols) == 1)) &&
       (only(source_cols) == columnindex(parent(gd), out_col_name) ||
        only(wfun) === identity || only(wfun) === copy)
        metacol = only(source_cols)
    else
        metacol = 0
    end

    return function()
        if haskey(seen_cols, out_col_name)
            # if column was seen and it is optional now ignore it
            if !optional_i
                optional, loc = seen_cols[out_col_name]
                # we have seen this col but it is not allowed to replace it
                optional || throw(ArgumentError("duplicate output column name: :$out_col_name"))
                @assert trans_res[loc].optional && trans_res[loc].name == out_col_name
                trans_res[loc] = TransformationResult(idx, outcol, out_col_name, optional_i, metacol)
                seen_cols[out_col_name] = (optional_i, loc)
            end
        else
            push!(trans_res, TransformationResult(idx, outcol, out_col_name, optional_i, metacol))
            seen_cols[out_col_name] = (optional_i, length(trans_res))
        end
    end
end

@noinline function expand_res_astable(res, kp1, emptyres::Bool)
    prepend = all(x -> x isa Integer, kp1)
    if !(prepend || all(x -> x isa Symbol, kp1) || all(x -> x isa AbstractString, kp1))
        throw(ArgumentError("keys of the returned elements must be " *
                            "`Symbol`s, strings or integers"))
    end
    if any(x -> !isequal(keys(x), kp1), res)
        throw(ArgumentError("keys of the returned elements must be equal"))
    end
    outcols = [[x[n] for x in res] for n in kp1]
    # make sure we only infer column names and types for empty res, but do not
    # produce values that were generated when computing firstres
    emptyres && foreach(empty!, outcols)
    nms = [prepend ? Symbol("x", n) : Symbol(n) for n in kp1]
    return outcols, nms
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
                                       wincols::Ref{Any},
                                       threads::Bool)
    fun = only(wfun)
    @assert fun isa Base.Callable
    @assert only(wincols) isa Union{Tuple, NamedTuple}
    if firstres isa AbstractVector
        idx, outcol_vec, _ = _combine_with_first(Ref{Any}(wrap(firstres)), wfun, gd, wincols,
                                                 firstmulticol, NOTHING_IDX_AGG, threads)
        @assert length(outcol_vec) == 1
        res = outcol_vec[1]
        if isempty(res)
            emptyres = true
            res = firstres
        else
            emptyres = false
        end
        kp1 = isempty(res) ? () : keys(res[1])

        outcols, nms = expand_res_astable(res, kp1, emptyres)
    else
        if !firstmulticol
            firstres = Tables.columntable(firstres)
            oldfun = fun
            fun = (x...) -> Tables.columntable(oldfun(x...))
        end
        idx, outcols, nms = _combine_multicol(Ref{Any}(firstres), Ref{Any}(fun), gd,
                                              wincols, threads)
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
    end
    @assert length(outcols) == length(nms)
    if out_col_name isa AbstractVector{Symbol}
        if length(out_col_name) != length(nms)
            throw(ArgumentError("Number of returned columns is $(length(nms)) " *
                                "and it does not match the number of provided output " *
                                "names which is $(length(out_col_name))"))
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
                    trans_res[loc] = TransformationResult(idx, outcol, out_col_name, optional_i, 0)
                    seen_cols[out_col_name] = (optional_i, loc)
                end
            else
                push!(trans_res, TransformationResult(idx, outcol, out_col_name, optional_i, 0))
                seen_cols[out_col_name] = (optional_i, length(trans_res))
            end
        end
    end
end

# helper function allowing us to identify groupindices and proprow transforms
function isspecialtransform((cs_i,)::Ref{Any})
    cs_i isa Pair || return false
    (first(cs_i) isa Vector{Int} && isempty(first(cs_i))) || return false
    fun = first(last(cs_i))
    fun === groupindices && return true
    fun === proprow && return true
    return false
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
                               idx_agg::Ref{Vector{Int}},
                               threads::Bool)
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
                                            Ref{Any}(fun), Ref{Any}(incols), threads, Ref{Any}(source_cols))
    end
    if out_col_name == AsTable || out_col_name isa AbstractVector{Symbol}
        return _combine_process_pair_astable(optional_i, gd, seen_cols, trans_res, idx_agg,
                                             out_col_name, firstmulticol, Ref{Any}(firstres),
                                             Ref{Any}(fun), Ref{Any}(incols), threads)
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
                  copycols::Bool, keeprows::Bool, renamecols::Bool,
                  threads::Bool)
    if isempty(cs_norm)
        if keeprows && nrow(parent(gd)) > 0 && minimum(gd.groups) == 0
            throw(ArgumentError("select and transform do not support " *
                                "`GroupedDataFrame`s from which some groups have "*
                                "been dropped (including skipmissing=true)"))
        end
        return Int[], DataFrame(), Int[]
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
    if any(x -> isagg(x, gd) || isspecialtransform(Ref{Any}(x)), cs_norm)
        # Compute indices of representative rows only once for all AbstractAggregates
        # or special transforms
        idx_agg[] = Vector{Int}(undef, length(gd))
        fillfirst!(nothing, idx_agg[], 1:length(gd.groups), gd)
    end
    if length(gd) == 0 || !all(x -> isagg(x, gd) || isspecialtransform(Ref{Any}(x)), cs_norm)
        # Trigger computation of indices
        # This can speed up some aggregates that would not trigger this on their own
        @assert length(gd.idx) >= 0
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
        tasks[i] = @spawn_or_run_task threads if length(gd) > 0 && isagg(cs_i, gd)
            _combine_process_agg(Ref{Any}(cs_i), optional_i, parentdf, gd,
                                 seen_cols, trans_res, idx_agg[])
        elseif keeprows && cs_i isa Pair && first(last(cs_i)) === identity &&
               !(first(cs_i) isa AsTable) && (last(last(cs_i)) isa Symbol)
            # this is a fast path used when
            # we pass a column or rename a column in select or transform
            _combine_process_noop(cs_i, optional_i, parentdf,
                                  seen_cols, trans_res, idx_keeprows, copycols)
        elseif cs_i isa Base.Callable
            _combine_process_callable(Ref{Any}(cs_i), optional_i, parentdf, gd,
                                      seen_cols, trans_res, idx_agg, threads)
        else
            @assert cs_i isa Pair
            if first(cs_i) isa Vector{Int} && isempty(first(cs_i)) &&
               first(last(cs_i)) === groupindices
                _combine_process_groupindices(Ref{Any}(cs_i), optional_i, parentdf, gd,
                                              seen_cols, trans_res, idx_agg[])
            elseif first(cs_i) isa Vector{Int} && isempty(first(cs_i)) &&
                   first(last(cs_i)) === proprow
                _combine_process_proprow(Ref{Any}(cs_i), optional_i, parentdf, gd,
                                         seen_cols, trans_res, idx_agg[])
            else
                _combine_process_pair(Ref{Any}(cs_i), optional_i, parentdf, gd,
                                      seen_cols, trans_res, idx_agg, threads)
            end
        end
    end
    # Workaround JuliaLang/julia#38931:
    # we want to preserve the exception type thrown in user code,
    # and print the backtrace corresponding to it
    for t in tasks
        try
            wait(t)
        catch e
            if e isa TaskFailedException
                throw(t.exception)
            else
                rethrow(e)
            end
        end
    end
    # Post-processing has to be run sequentially
    # since the order of operations determines that of columns
    for t in tasks
        postprocessf = fetch(t)
        postprocessf()
    end

    isempty(trans_res) && return Int[], DataFrame(), Int[]
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
                                                        trans_res[i].name, trans_res[i].optional,
                                                        trans_res[i].metadatacol)
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
            @spawn_or_run threads reorder_cols!(trans_res, i, trans_res[i].col, trans_res[i].col_idx,
                                                keeprows, idx_keeprows, gd)
        end
    end

    outcols = AbstractVector[x.col for x in trans_res]
    nms = Symbol[x.name for x in trans_res]
    metacols = Int[x.metadatacol for x in trans_res]
    # this check is redundant given we check idx above
    # but it is safer to double check and it is cheap
    @assert all(x -> length(x) == length(outcols[1]), outcols)
    return idx, DataFrame(outcols, nms, copycols=false), metacols
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
        trans_res[i] = TransformationResult(col_idx, newcol, trans_res[i].name,
                                            trans_res[i].optional, trans_res[i].metadatacol)
    end
end

function combine(@nospecialize(f::Base.Callable), gd::GroupedDataFrame;
                 keepkeys::Bool=true, ungroup::Bool=true, renamecols::Bool=true,
                 threads::Bool=true)
    if f isa Colon
        throw(ArgumentError("First argument must be a transformation if the second argument is a GroupedDataFrame"))
    end
    return combine(gd, f, keepkeys=keepkeys, ungroup=ungroup, renamecols=renamecols,
                   threads=threads)
end

combine(@nospecialize(f::Pair), gd::GroupedDataFrame;
        keepkeys::Bool=true, ungroup::Bool=true, renamecols::Bool=true,
        threads::Bool=true) =
    throw(ArgumentError("First argument must be a transformation if the second argument is a GroupedDataFrame. " *
                        "You can pass a `Pair` as the second argument of the transformation. If you want the return " *
                        "value to be processed as having multiple columns add `=> AsTable` suffix to the pair."))

combine(gd::GroupedDataFrame,
        @nospecialize(args::Union{Pair, Base.Callable, ColumnIndex, MultiColumnIndex,
                                  AbstractVecOrMat}...);
        keepkeys::Bool=true, ungroup::Bool=true, renamecols::Bool=true,
        threads::Bool=true) =
    _combine_prepare(gd, Ref{Any}(map(x -> broadcast_pair(parent(gd), x), args)),
                     keepkeys=keepkeys, ungroup=ungroup,
                     copycols=true, keeprows=false, renamecols=renamecols,
                     threads=threads)

function _dealias_dataframe!(df::DataFrame)
    seen_cols = IdDict{Any, Nothing}()
    for (i, col) in enumerate(eachcol(df))
        if !haskey(seen_cols, col)
            seen_cols[col] = nothing
        else
            df[!, i] = df[:, i]
        end
    end
end

function select(@nospecialize(f::Base.Callable), gd::GroupedDataFrame; copycols::Bool=true,
                keepkeys::Bool=true, ungroup::Bool=true, renamecols::Bool=true,
                threads::Bool=true)
    if f isa Colon
        throw(ArgumentError("First argument must be a transformation if the second argument is a grouped data frame"))
    end
    return select(gd, f, copycols=copycols, keepkeys=keepkeys, ungroup=ungroup,
                  threads=threads)
end

function select(gd::GroupedDataFrame, @nospecialize(args::Union{Pair, Base.Callable, ColumnIndex,
                                                                MultiColumnIndex, AbstractVecOrMat}...);
                copycols::Bool=true, keepkeys::Bool=true, ungroup::Bool=true, renamecols::Bool=true,
                threads::Bool=true)
    res = _combine_prepare(gd, Ref{Any}(map(x -> broadcast_pair(parent(gd), x), args)),
                           copycols=copycols, keepkeys=keepkeys,
                           ungroup=ungroup, keeprows=true, renamecols=renamecols,
                           threads=threads)
    # res can be a GroupedDataFrame based on DataFrame or a DataFrame,
    # so parent always gives a DataFrame
    copycols || _dealias_dataframe!(parent(res))
    return res
end

function transform(@nospecialize(f::Base.Callable), gd::GroupedDataFrame; copycols::Bool=true,
                   keepkeys::Bool=true, ungroup::Bool=true, renamecols::Bool=true,
                   threads::Bool=true)
    if f isa Colon
        throw(ArgumentError("First argument must be a transformation if the second argument is a grouped data frame"))
    end
    return transform(gd, f, copycols=copycols, keepkeys=keepkeys, ungroup=ungroup,
                     threads=threads)
end

function transform(gd::GroupedDataFrame, @nospecialize(args::Union{Pair, Base.Callable, ColumnIndex, MultiColumnIndex,
                                                                   AbstractVecOrMat}...);
                   copycols::Bool=true, keepkeys::Bool=true, ungroup::Bool=true,
                   renamecols::Bool=true,
                   threads::Bool=true)
    res = select(gd, :, args..., copycols=copycols, keepkeys=keepkeys,
                 ungroup=ungroup, renamecols=renamecols, threads=threads)
    # res can be a GroupedDataFrame based on DataFrame or a DataFrame,
    # so parent always gives a data frame
    select!(parent(res), propertynames(parent(gd)), :, threads=threads)
    return res
end

function select!(@nospecialize(f::Base.Callable), gd::GroupedDataFrame;
                 ungroup::Bool=true, renamecols::Bool=true, threads::Bool=true)
    if f isa Colon
        throw(ArgumentError("First argument must be a transformation if the second argument is a grouped data frame"))
    end
    return select!(gd, f, ungroup=ungroup, threads=threads)
end

function select!(gd::GroupedDataFrame,
                 @nospecialize(args::Union{Pair, Base.Callable, ColumnIndex, MultiColumnIndex,
                                           AbstractVecOrMat}...);
                 ungroup::Bool=true, renamecols::Bool=true, threads::Bool=true)
    df = parent(gd)
    if df isa DataFrame
        newdf = select(gd, args..., copycols=false, renamecols=renamecols,
                       threads=threads)
        _replace_columns!(df, newdf)
    else
        @assert df isa SubDataFrame
        newdf = select(gd, args..., copycols=true, renamecols=renamecols,
                       threads=threads)
        _replace_columns!(df, newdf, keep_present=false)
    end
    return ungroup ? df : gd
end

function transform!(@nospecialize(f::Base.Callable), gd::GroupedDataFrame;
                    ungroup::Bool=true, renamecols::Bool=true, threads::Bool=true)
    if f isa Colon
        throw(ArgumentError("First argument must be a transformation if the second argument is a grouped data frame"))
    end
    return transform!(gd, f, ungroup=ungroup)
end

function transform!(gd::GroupedDataFrame,
                    @nospecialize(args::Union{Pair, Base.Callable, ColumnIndex, MultiColumnIndex,
                                              AbstractVecOrMat}...);
                    ungroup::Bool=true, renamecols::Bool=true, threads::Bool=true)
    df = parent(gd)
    if df isa DataFrame
        newdf = select(gd, :, args..., copycols=false, renamecols=renamecols,
                       threads=threads)
        # need to recover column order of df in newdf and add new columns at the end
        select!(newdf, propertynames(df), :, threads=threads)
        _replace_columns!(df, newdf)
    else
        @assert df isa SubDataFrame
        newdf = select(gd, args..., copycols=true, renamecols=renamecols,
                       threads=threads)
        # here column order of df is retained due to keep_present=true
        _replace_columns!(df, newdf, keep_present=true)
    end
    return ungroup ? df : gd
end
