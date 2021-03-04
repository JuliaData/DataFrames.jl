struct OnColRow{T}
    row::Int
    cols::T
    h::Vector{UInt}

    OnColRow(row::Union{Signed,Unsigned},
             cols::NTuple{<:Any, AbstractVector}, h::Vector{UInt}) =
        new{typeof(cols)}(Int(row), cols, h)
end

struct OnCol{T,N} <: AbstractVector{OnColRow{T}}
    len::Int
    cols::T
    h::Vector{UInt}

    function OnCol(cs::AbstractVector...)
        @assert length(cs) > 1
        len = length(cs[1])
        @assert all(x -> firstindex(x) == 1, cs)
        @assert all(x -> lastindex(x) == len, cs)
        new{typeof(cs), length(cs)}(len, cs, UInt[])
    end
end

Base.IndexStyle(::Type{<:OnCol}) = Base.IndexLinear()

@inline Base.size(oc::OnCol) = (oc.len,)

@inline function Base.getindex(oc::OnCol, i::Int)
    @boundscheck checkbounds(oc, i)
    return OnColRow(i, oc.cols, oc.h)
end

Base.hash(ocr1::OnColRow, h::UInt) = throw(MethodError(hash, (ocr1, h)))
@inline Base.hash(ocr1::OnColRow) = @inbounds ocr1.h[ocr1.row]

# Hashing one column at a time is faster since it can use SIMD
function _prehash(oc::OnCol)
    h = oc.h
    resize!(h, oc.len)
    fill!(h, Base.tuplehash_seed)
    for col in reverse(oc.cols)
        h .= hash.(col, h)
    end
end

# TODO: rewrite isequal and isless to use @generated
# or some other approach that would keep them efficient and avoid code duplication

Base.:(==)(x::OnColRow, y::OnColRow) = throw(MethodError(==, (x, y)))

@inline function Base.isequal(ocr1::OnColRow{<:NTuple{2, AbstractVector}},
                              ocr2::OnColRow{<:NTuple{2, AbstractVector}})
    r1 = ocr1.row
    c11, c12 = ocr1.cols
    r2 = ocr2.row
    c21, c22 = ocr2.cols

    return @inbounds isequal(c11[r1], c21[r2]) && isequal(c12[r1], c22[r2])
end

Base.isequal(ocr1::OnColRow{<:NTuple{N,AbstractVector}},
             ocr2::OnColRow{<:NTuple{N,AbstractVector}}) where {N} =
    isequal(ntuple(i -> @inbounds(ocr1.cols[i][ocr1.row]), N),
            ntuple(i -> @inbounds(ocr2.cols[i][ocr2.row]), N))

@inline function Base.isless(ocr1::OnColRow{<:NTuple{2, AbstractVector}},
                             ocr2::OnColRow{<:NTuple{2, AbstractVector}})
    r1 = ocr1.row
    c11, c12 = ocr1.cols
    r2 = ocr2.row
    c21, c22 = ocr2.cols

    c11r = @inbounds c11[r1]
    c12r = @inbounds c12[r1]
    c21r = @inbounds c21[r2]
    c22r = @inbounds c22[r2]

    isless(c11r, c21r) || (isequal(c11r, c21r) && isless(c12r, c22r))
end

@inline Base.isless(ocr1::OnColRow{<:NTuple{N,AbstractVector}},
                    ocr2::OnColRow{<:NTuple{N,AbstractVector}}) where {N} =
    isless(ntuple(i -> @inbounds(ocr1.cols[i][ocr1.row]), N),
           ntuple(i -> @inbounds(ocr2.cols[i][ocr2.row]), N))

prepare_on_col() = throw(ArgumentError("at least one on column required when joining"))
prepare_on_col(c::AbstractVector) = c
prepare_on_col(cs::AbstractVector...) = OnCol(cs...)

# Return if it is allowed to use refpool instead of the original array for joining.
# There are multiple conditions that must be met to allow for this.
# If it is allowed we are sure that missing can be used as a sentinel
check_mapping_allowed(short::AbstractVector, refarray_long::AbstractVector,
                      refpool_long, invrefpool_long) =
    !isempty(short) && !isnothing(refpool_long) && !isnothing(invrefpool_long) &&
        eltype(refarray_long) <: Union{Signed, Unsigned}

@noinline map_refarray(mapping::AbstractVector, refarray::AbstractVector, ::Val{fi})  where {fi} =
    [@inbounds mapping[r - fi + 1] for r in refarray]

function map2refs(x::AbstractVector, invrefpool)
    x_refpool = DataAPI.refpool(x)
    if x_refpool isa AbstractVector{<:Integer} && 0 <= firstindex(x_refpool) <= 1
        # here we know that x_refpool is AbstractVector that allows integer indexing
        # and its firstindex must be an integer
        # if firstindex is not 0 or 1 then we fallback to slow path for safety reasons
        # all refpool we currently know have firstindex 0 or 1
        # if there is some very strange firstindex we might run into overflow issues
        # below use function barrier as mapping is not type stable
        mapping = [get(invrefpool, v, missing) for v in x_refpool]
        return map_refarray(mapping, DataAPI.refarray(x), Val(Int(firstindex(x_refpool))))
    else
        return [get(invrefpool, v, missing) for v in x]
    end
end

@inline function find_next_range(x::AbstractArray, start::Int, start_value)
    stop_value = start_value
    n = length(x)
    stop = start + 1
    while stop <= n
        @inbounds stop_value = x[stop]
        isequal(start_value, stop_value) || break
        stop += 1
    end
    return stop, stop_value
end

function _innerjoin_sorted(left::AbstractArray, right::AbstractArray)
    left_n = length(left)
    right_n = length(right)

    left_ixs = Int[]
    right_ixs = Int[]

    (left_n == 0 || right_n == 0) && return left_ixs, right_ixs

    # lower bound assuming we get matches
    sizehint!(left_ixs, min(left_n, right_n))
    sizehint!(right_ixs, min(left_n, right_n))

    left_cur = 1
    left_val = left[left_cur]
    left_new, left_tmp = find_next_range(left, left_cur, left_val)

    right_cur = 1
    right_val = right[right_cur]
    right_new, right_tmp = find_next_range(right, right_cur, right_val)

    while left_cur <= left_n && right_cur <= right_n
        if isequal(left_val, right_val)
            if left_new - left_cur == right_new - right_cur == 1
                push!(left_ixs, left_cur)
                push!(right_ixs, right_cur)
            else
                idx = length(left_ixs)
                left_range = left_cur:left_new - 1
                right_range = right_cur:right_new - 1
                to_grow = Base.checked_add(idx, Base.checked_mul(length(left_range), length(right_range)))
                resize!(left_ixs, to_grow)
                resize!(right_ixs, to_grow)
                @inbounds for right_i in right_range, left_i in left_range
                    idx += 1
                    left_ixs[idx] = left_i
                    right_ixs[idx] = right_i
                end
            end
            left_cur, left_val = left_new, left_tmp
            left_new, left_tmp = find_next_range(left, left_cur, left_val)
            right_cur, right_val = right_new, right_tmp
            right_new, right_tmp = find_next_range(right, right_cur, right_val)
        elseif isless(left_val, right_val)
            left_cur, left_val = left_new, left_tmp
            left_new, left_tmp = find_next_range(left, left_cur, left_val)
        else
            right_cur, right_val = right_new, right_tmp
            right_new, right_tmp = find_next_range(right, right_cur, right_val)
        end
    end

    return left_ixs, right_ixs
end

# optimistically assume that shorter table does not have duplicates in on column
# if this is not the case we call _innerjoin_dup
# which efficiently uses the work already done and continues with the more
# memory expensive algorithm that allows for duplicates
function _innerjoin_unsorted(left::AbstractArray, right::AbstractArray{T}) where {T}
    dict = Dict{T, Int}()

    right_len = length(right)
    sizehint!(dict, 2 * min(right_len, typemax(Int) >> 2))

    right isa OnCol && _prehash(right)
    left isa OnCol && _prehash(left)

    for (idx_r, val_r) in enumerate(right)
        haskey(dict, val_r) && return _innerjoin_dup(left, right, dict, idx_r)
        dict[val_r] = idx_r
    end

    left_ixs = Int[]
    right_ixs = Int[]

    # lower bound assuming we get matches
    sizehint!(left_ixs, right_len)
    sizehint!(right_ixs, right_len)

    for (idx_l, val_l) in enumerate(left)
        # we know that dict contains only positive values
        idx_r = get(dict, val_l, -1)
        if idx_r != -1
            push!(left_ixs, idx_l)
            push!(right_ixs, idx_r)
        end
    end
    return left_ixs, right_ixs
end

extrema_missing(x::AbstractVector{Missing}) = (1, 0)

function extrema_missing(x::AbstractVector{T}) where {T<:Union{Integer, Missing}}
    try
        return extrema(skipmissing(x))
    catch
        S = nonmissingtype(T)
        return S(1), S(0)
    end
end

function _innerjoin_unsorted_int(left::AbstractVector{<:Union{Integer, Missing}},
                                 right::AbstractVector{<:Union{Integer, Missing}})
    minv, maxv = extrema_missing(right)

    val_range = big(maxv) - big(minv)
    if val_range > typemax(Int) - 3 || val_range ÷ 2 > max(64, length(right)) ||
       minv < typemin(Int) + 2 || maxv > typemax(Int) - 3
       return _innerjoin_unsorted(left, right)
    end

    offset = 1 - Int(minv) # we are now sure it does not overflow
    len = Int(maxv) - Int(minv) + 2
    dict = zeros(Int, len)

    @inbounds for (idx_r, val_r) in enumerate(right)
        i = ismissing(val_r) ? length(dict) : Int(val_r) + offset
        if dict[i] > 0
            return _innerjoin_dup_int(left, right, dict, idx_r, offset, Int(minv), Int(maxv))
        end
        dict[i] = idx_r
    end

    left_ixs = Int[]
    right_ixs = Int[]

    right_len = length(right)
    sizehint!(left_ixs, right_len)
    sizehint!(right_ixs, right_len)

    @inbounds for (idx_l, val_l) in enumerate(left)
        # we use dict_index to make sure the following two operations are fast:
        # - if index is found - get it and process it
        # - if index is not found - do nothing
        if ismissing(val_l)
            idx_r = dict[end]
            if idx_r > 0
                push!(left_ixs, idx_l)
                push!(right_ixs, idx_r)
            end
        elseif minv <= val_l <= maxv
            idx_r = dict[Int(val_l) + offset]
            if idx_r > 0
                push!(left_ixs, idx_l)
                push!(right_ixs, idx_r)
            end
        end
    end
    return left_ixs, right_ixs
end

# we fall back to general case if we have duplicates
# normally it should happen fast as we reuse work already done
function _innerjoin_dup(left::AbstractArray, right::AbstractArray{T},
                        dict::Dict{T, Int}, idx_r_start::Int) where {T}
    ngroups = idx_r_start - 1
    right_len = length(right)
    groups = Vector{Int}(undef, right_len)
    groups[1:ngroups] = 1:ngroups

    @inbounds for idx_r in idx_r_start:right_len
        val_r = right[idx_r]
        # we know that group ids are positive
        group_id = get(dict, val_r, -1)
        if group_id == -1
            ngroups += 1
            groups[idx_r] = ngroups
            dict[val_r] = ngroups
        else
            groups[idx_r] = group_id
        end
    end

    @assert ngroups > 0 # we should not get here with 0-length right
    return _innerjoin_postprocess(left, dict, groups, ngroups, right_len)
end

function _innerjoin_dup_int(left::AbstractVector{<:Union{Integer, Missing}},
                            right::AbstractVector{<:Union{Integer, Missing}},
                            dict::Vector{Int}, idx_r_start::Int, offset::Int,
                            minv::Int, maxv::Int)
    ngroups = idx_r_start - 1
    right_len = length(right)
    groups = Vector{Int}(undef, right_len)
    groups[1:ngroups] = 1:ngroups

    @inbounds for idx_r in idx_r_start:right_len
        val_r = right[idx_r]
        i = ismissing(val_r) ? length(dict) : Int(val_r) + offset
        dict_val = dict[i]
        if dict_val > 0
            groups[idx_r] = dict_val
        else
            ngroups += 1
            groups[idx_r] = ngroups
            dict[i] = ngroups
        end
    end

    @assert ngroups > 0 # we should not get here with 0-length right
    return _innerjoin_postprocess_int(left, dict, groups, ngroups, right_len, offset, minv, maxv)
end

function compute_join_indices!(groups::Vector{Int}, ngroups::Int,
                               starts::Vector, rperm::Vector)
    @inbounds for gix in groups
        starts[gix] += 1
    end

    cumsum!(starts, starts)

    @inbounds for (i, gix) in enumerate(groups)
        rperm[starts[gix]] = i
        starts[gix] -= 1
    end
    push!(starts, length(groups))
    return nothing
end

function _innerjoin_postprocess(left::AbstractArray, dict::Dict{T, Int},
                                groups::Vector{Int}, ngroups::Int, right_len::Int) where {T}
    starts = zeros(Int, ngroups)
    rperm = Vector{Int}(undef, right_len)

    left_ixs = Int[]
    right_ixs = Int[]

    # lower bound assuming we get matches
    sizehint!(left_ixs, right_len)
    sizehint!(right_ixs, right_len)

    compute_join_indices!(groups, ngroups, starts, rperm)

    n = 0
    @inbounds for (idx_l, val_l) in enumerate(left)
        group_id = get(dict, val_l, -1)
        if group_id != -1
            ref_stop = starts[group_id + 1]
            l = ref_stop - starts[group_id]
            newn = n + l
            resize!(left_ixs, newn)
            for i in n+1:n+l
                left_ixs[i] = idx_l
            end
            resize!(right_ixs, newn)
            for i in 1:l
                right_ixs[n + i] = rperm[ref_stop - i + 1]
            end
            n = newn
        end
    end

    return left_ixs, right_ixs
end

function _innerjoin_postprocess_int(left::AbstractVector{<:Union{Integer, Missing}},
                                    dict::Vector{Int},
                                    groups::Vector{Int}, ngroups::Int, right_len::Int,
                                    offset::Int, minv::Int, maxv::Int)
    starts = zeros(Int, ngroups)
    rperm = Vector{Int}(undef, right_len)

    left_ixs = Int[]
    right_ixs = Int[]

    sizehint!(left_ixs, right_len)
    sizehint!(right_ixs, right_len)

    compute_join_indices!(groups, ngroups, starts, rperm)

    n = 0
    @inbounds for (idx_l, val_l) in enumerate(left)
        if ismissing(val_l)
            group_id = dict[end]
        elseif minv <= val_l <= maxv
            group_id = dict[Int(val_l) + offset]
        else
            group_id = 0
        end

        if group_id > 0
            ref_stop = starts[group_id + 1]
            l = ref_stop - starts[group_id]
            newn = n + l
            resize!(left_ixs, newn)
            for i in n+1:n+l
                left_ixs[i] = idx_l
            end
            resize!(right_ixs, newn)
            for i in 1:l
                right_ixs[n + i] = rperm[ref_stop - i + 1]
            end
            n = newn
        end
    end

    return left_ixs, right_ixs
end

function find_inner_rows(joiner::DataFrameJoiner)
    right_len = length(joiner.dfr_on[!, 1])
    left_len = length(joiner.dfl_on[!, 1])
    right_shorter = right_len <= left_len

    left_cols = collect(eachcol(joiner.dfl_on))
    right_cols = collect(eachcol(joiner.dfr_on))

    # if column of a longer table supports DataAPI.refpool and DataAPI.invrefpool
    # remap matching left and right columns to use refs
    if right_shorter
        for i in eachindex(left_cols, right_cols)
            rc = right_cols[i]
            lc = left_cols[i]

            lc_refs = DataAPI.refarray(lc)
            lc_refpool = DataAPI.refpool(lc)
            lc_invrefpool = DataAPI.invrefpool(lc)
            if check_mapping_allowed(rc, lc_refs, lc_refpool, lc_invrefpool)
                right_cols[i] = map2refs(rc, lc_invrefpool)
                left_cols[i] = lc_refs
            end
        end
    else
        for i in eachindex(left_cols, right_cols)
            rc = right_cols[i]
            lc = left_cols[i]

            rc_refs = DataAPI.refarray(rc)
            rc_refpool = DataAPI.refpool(rc)
            rc_invrefpool = DataAPI.invrefpool(rc)
            if check_mapping_allowed(lc, rc_refs, rc_refpool, rc_invrefpool)
                right_cols[i] = rc_refs
                left_cols[i] = map2refs(lc, rc_invrefpool)
            end
        end
    end

    disallow_sorted = false

    for (lc, rc) in zip(left_cols, right_cols)
        @assert length(lc) == left_len
        @assert length(rc) == right_len
        lc_et = nonmissingtype(eltype(lc))
        rc_et = nonmissingtype(eltype(rc))

        # special case common safe scenarios when eltype between left and right column
        # can be different or non-concrete
        lc_et <: Real && rc_et <: Real && continue
        lc_et <: AbstractString && rc_et <: AbstractString && continue

        # otherwise we require non-missing eltype of both sides to be the same and concrete
        lc_et === rc_et && isconcretetype(lc_et) && continue

        # we disallow using sorted branch for some columns that theoretically
        # could be safely sorted (e.g. having Any eltype but holding strings)
        # for safety reasons assuming that such cases will be rare in practice
        disallow_sorted = true
    end

    # TODO:
    # If DataAPI.invrefpool vectors are found in the "on" columns
    # then potentially the following optimizations can be done:
    # 1. identify rows in shorter table that should be dropped
    # 2. develop custom _innerjoin_sorted and _innerjoin_unsorted that
    #    drop rows from shorter table that do not match rows from longer table based on
    #    PooledArray refpool check
    # This optimization significantly complicates the code (especially sorted path).
    # It should be added if in practice we find that the use case is often enough
    # and that the benefits are significant. The two cases when the benefits should
    # be expected are:
    # 1. Shorter table is sorted when we drop rows not matching longer table rows
    # 2. Shorter table does not have duplicates when we drop rows not matching longer table rows

    left_col = prepare_on_col(left_cols...)
    right_col = prepare_on_col(right_cols...)

    # we treat this case separately so we know we have at least one element later
    (isempty(left_col) || isempty(right_col)) && return Int[], Int[]

    # if sorting is not disallowed try using a fast algorithm that works
    # on sorted columns; if it is not run or errors fall back to the unsorted case
    # the try-catch is used to handle the case when columns on which we join
    # contain values that are not comparable
    if !disallow_sorted
        try
            if issorted(left_col) && issorted(right_col)
                return _innerjoin_sorted(left_col, right_col)
            end
        catch
            # nothing to do - one of the columns is not sortable
        end
    end

    if right_shorter
        if left_col isa AbstractVector{<:Union{Integer, Missing}} &&
           right_col isa AbstractVector{<:Union{Integer, Missing}}
            return _innerjoin_unsorted_int(left_col, right_col)
        else
            return _innerjoin_unsorted(left_col, right_col)
        end
    else
        if left_col isa AbstractVector{<:Union{Integer, Missing}} &&
           right_col isa AbstractVector{<:Union{Integer, Missing}}
            return reverse(_innerjoin_unsorted_int(right_col, left_col))
        else
            return reverse(_innerjoin_unsorted(right_col, left_col))
        end
    end

    error("unreachable reached")
end
