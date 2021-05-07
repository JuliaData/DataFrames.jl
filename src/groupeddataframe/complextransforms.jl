_nrow(df::AbstractDataFrame) = nrow(df)
_nrow(x::NamedTuple{<:Any, <:Tuple{Vararg{AbstractVector}}}) =
    isempty(x) ? 0 : length(x[1])
_ncol(df::AbstractDataFrame) = ncol(df)
_ncol(x::Union{NamedTuple, DataFrameRow}) = length(x)

function _combine_multicol((firstres,)::Ref{Any}, wfun::Ref{Any}, gd::GroupedDataFrame,
                           wincols::Ref{Any})
    @assert only(wfun) isa Base.Callable
    @assert only(wincols) isa Union{Nothing, AbstractVector, Tuple, NamedTuple}
    firstmulticol = firstres isa MULTI_COLS_TYPE
    if !(firstres isa Union{AbstractVecOrMat, AbstractDataFrame,
                            NamedTuple{<:Any, <:Tuple{Vararg{AbstractVector}}}})
        idx_agg = Vector{Int}(undef, length(gd))
        fillfirst!(nothing, idx_agg, 1:length(gd.groups), gd)
    else
        idx_agg = NOTHING_IDX_AGG
    end
    return _combine_with_first(Ref{Any}(wrap(firstres)), wfun, gd, wincols,
                               firstmulticol, idx_agg)
end

function _combine_with_first((first,)::Ref{Any},
                             (f,)::Ref{Any}, gd::GroupedDataFrame,
                             (incols,)::Ref{Any},
                             firstmulticol::Bool, idx_agg::Vector{Int})
    @assert first isa Union{NamedTuple, DataFrameRow, AbstractDataFrame}
    @assert f isa Base.Callable
    @assert incols isa Union{Nothing, AbstractVector, Tuple, NamedTuple}
    @assert first isa Union{NamedTuple, DataFrameRow, AbstractDataFrame}
    extrude = false

    lgd = length(gd)
    if first isa AbstractDataFrame
        n = 0
        eltys = eltype.(eachcol(first))
    elseif first isa NamedTuple{<:Any, <:Tuple{Vararg{AbstractVector}}}
        n = 0
        eltys = map(eltype, first)
    elseif first isa DataFrameRow
        n = lgd
        eltys = [eltype(parent(first)[!, i]) for i in parentcols(index(first))]
    elseif !firstmulticol && first[1] isa Union{AbstractArray{<:Any, 0}, Ref}
        extrude = true
        first = wrap_row(first[1], firstcoltype(firstmulticol))
        n = lgd
        eltys = (typeof(first[1]),)
    else # other NamedTuple giving a single row
        n = lgd
        eltys = map(typeof, first)
        if any(x -> x <: AbstractVector, eltys)
            throw(ArgumentError("mixing single values and vectors in a named tuple is not allowed"))
        end
    end

    idx = idx_agg === NOTHING_IDX_AGG ? Vector{Int}(undef, n) : idx_agg
    # assume that we will have at least one row per group;
    # if the user wants to drop some groups this will over-allocate idx
    # but this use case is uncommon and sizehint! is cheap.
    sizehint!(idx, lgd)

    local initialcols
    let eltys=eltys, n=n # Workaround for julia#15276
        initialcols = ntuple(i -> Tables.allocatecolumn(eltys[i], n), _ncol(first))
    end
    targetcolnames = tuple(propertynames(first)...)
    if !extrude && first isa Union{AbstractDataFrame,
                                   NamedTuple{<:Any, <:Tuple{Vararg{AbstractVector}}}}
        outcols, finalcolnames = _combine_tables_with_first!(first, initialcols, idx, 1, 1,
                                                             f, gd, incols, targetcolnames,
                                                             firstcoltype(firstmulticol))
    else
        outcols, finalcolnames = _combine_rows_with_first!(Ref{Any}(first),
                                                           Ref{Any}(initialcols),
                                                           Ref{Any}(f),
                                                           gd,
                                                           Ref{Any}(incols),
                                                           Ref{Any}(targetcolnames),
                                                           firstmulticol)
    end
    return idx, outcols, collect(Symbol, finalcolnames)
end

function _names_match(r1, t::NTuple{N, Symbol}) where N
    n1 = _getnames(r1)
    length(n1) == length(t) || return false
    for (a, b) in zip(n1, t)
        a == b || return false
    end
    return true
end

function fill_row!(row, outcols::NTuple{N, AbstractVector},
                   i::Integer, colstart::Integer,
                   colnames::NTuple{N, Symbol}) where N
    if !_names_match(row, colnames)
        throw(ArgumentError("return value must have the same column names " *
                            "for all groups (got $colnames and $(propertynames(row)))"))
    end
    @inbounds for j in colstart:length(outcols)
        col = outcols[j]
        val = row[j]
        S = typeof(val)
        T = eltype(col)
        if S <: T || promote_type(S, T) <: T
            col[i] = val
        else
            return j
        end
    end
    return nothing
end

function _combine_rows_with_first_task!(tid::Integer,
                                        rowstart::Integer,
                                        rowend::Integer,
                                        rownext::Integer,
                                        outcols::NTuple{<:Any, AbstractVector},
                                        outcolsref::Ref{NTuple{<:Any, AbstractVector}},
                                        type_widened::AbstractVector{Bool},
                                        widen_type_lock::ReentrantLock,
                                        f::Base.Callable,
                                        gd::GroupedDataFrame,
                                        starts::AbstractVector{<:Integer},
                                        ends::AbstractVector{<:Integer},
                                        incols::Union{Nothing, AbstractVector,
                                                      Tuple, NamedTuple},
                                        colnames::NTuple{<:Any, Symbol},
                                        firstmulticol::FirstColCount)
    j = nothing
    gdidx = gd.idx
    local newoutcols
    for i in rownext:rowend
        row = wrap_row(do_call(f, gdidx, starts, ends, gd, incols, i),
                       firstmulticol)
        j = fill_row!(row, outcols, i, 1, colnames)
        if j !== nothing # Need to widen column
            # If another thread is already widening outcols, wait until it's done
            lock(widen_type_lock)
            try
                newoutcols = outcolsref[]
                # Workaround for julia#15276
                newoutcols = let i=i, j=j, newoutcols=newoutcols, row=row
                    ntuple(length(newoutcols)) do k
                        S = typeof(row[k])
                        T = eltype(newoutcols[k])
                        U = promote_type(S, T)
                        if S <: T || U <: T
                            newoutcols[k]
                        else
                            type_widened .= true
                            Tables.allocatecolumn(U, length(newoutcols[k]))
                        end
                    end
                end
                for k in 1:length(outcols)
                    if outcols[k] !== newoutcols[k]
                        copyto!(newoutcols[k], rowstart,
                                outcols[k], rowstart, i - rowstart + (k < j))
                    end
                end
                j = fill_row!(row, newoutcols, i, j, colnames)
                @assert j === nothing # eltype is guaranteed to match

                outcolsref[] = newoutcols
                type_widened[tid] = false
            finally
                unlock(widen_type_lock)
            end
            return _combine_rows_with_first_task!(tid, rowstart, rowend, i+1, newoutcols, outcolsref,
                                                  type_widened, widen_type_lock,
                                                  f, gd, starts, ends,
                                                  incols, colnames,
                                                  firstmulticol)
        end
        # If other thread widened columns, copy already processed data to new vectors
        # This doesn't have to happen immediately (hence type_widened isn't atomic),
        # but the more we wait the more data will have to be copied
        if type_widened[tid]
            lock(widen_type_lock) do
                type_widened[tid] = false
                newoutcols = outcolsref[]
                for k in 1:length(outcols)
                    # Check whether this particular column has been widened
                    if outcols[k] !== newoutcols[k]
                        copyto!(newoutcols[k], rowstart,
                                outcols[k], rowstart, i - rowstart + 1)
                    end
                end
            end
            return _combine_rows_with_first_task!(tid, rowstart, rowend, i+1, newoutcols, outcolsref,
                                                  type_widened, widen_type_lock,
                                                  f, gd, starts, ends,
                                                  incols, colnames,
                                                  firstmulticol)
        end
    end
    return outcols
end

# CategoricalArray is thread-safe only when input and output levels are equal
# since then all values are added upfront. Otherwise, if the function returns
# CategoricalValues mixed from different pools, one thread may add values,
# which may put the invpool in an invalid state while the other one is reading from it
function isthreadsafe(outcols::NTuple{<:Any, AbstractVector},
                      incols::Union{Tuple, NamedTuple})
    anycat = any(outcols) do col
        T = typeof(col)
        # If the first result is missing, widening can give a CategoricalArray
        # if later results are CategoricalValues
        eltype(col) === Missing ||
            (nameof(T) === :CategoricalArray &&
             nameof(parentmodule(T)) === :CategoricalArrays)
    end
    if anycat
        levs = nothing
        for col in incols
            T = typeof(col)
            if nameof(T) === :CategoricalArray &&
                nameof(parentmodule(T)) === :CategoricalArrays
                if levs !== nothing
                    levs == levels(col) || return false
                else
                    levs = levels(col)
                end
            end
        end
    end
    return true
end
isthreadsafe(outcols::NTuple{<:Any, AbstractVector}, incols::AbstractVector) =
    isthreadsafe(outcols, (incols,))
isthreadsafe(outcols::NTuple{<:Any, AbstractVector}, incols::Nothing) = true

function _combine_rows_with_first!((firstrow,)::Ref{Any},
                                   (outcols,)::Ref{Any},
                                   (f,)::Ref{Any},
                                   gd::GroupedDataFrame,
                                   (incols,)::Ref{Any},
                                   (colnames,)::Ref{Any},
                                   firstmulticol::Bool)
    @assert firstrow isa Union{NamedTuple, DataFrameRow}
    @assert outcols isa NTuple{N, AbstractVector} where N
    @assert f isa Base.Callable
    @assert incols isa Union{Nothing, AbstractVector, Tuple, NamedTuple}
    @assert colnames isa NTuple{N, Symbol} where N
    @assert length(colnames) == length(outcols)
    len = length(gd)
    gdidx = gd.idx
    starts = gd.starts
    ends = gd.ends

    # handle empty GroupedDataFrame
    len == 0 && return outcols, colnames

    # Handle first group
    j1 = fill_row!(firstrow, outcols, 1, 1, colnames)
    @assert j1 === nothing # eltype is guaranteed to match

    # Handle groups other than the first one
    # Create up to one task per thread
    # This has lower overhead than creating one task per group,
    # but is optimal only if operations take roughly the same time for all groups
    if VERSION >= v"1.4" && isthreadsafe(outcols, incols)
        basesize = max(1, cld(len - 1, Threads.nthreads()))
        partitions = Iterators.partition(2:len, basesize)
    else
        partitions = (2:len,)
    end
    widen_type_lock = ReentrantLock()
    outcolsref = Ref{NTuple{<:Any, AbstractVector}}(outcols)
    type_widened = fill(false, length(partitions))
    tasks = Vector{Task}(undef, length(partitions))
    for (tid, idx) in enumerate(partitions)
        tasks[tid] =
            @spawn _combine_rows_with_first_task!(tid, first(idx), last(idx), first(idx),
                                                  outcols, outcolsref,
                                                  type_widened, widen_type_lock,
                                                  f, gd, starts, ends, incols, colnames,
                                                  firstcoltype(firstmulticol))
    end

    # Workaround JuliaLang/julia#38931:
    # we want to preserve the exception type thrown in user code,
    # and print the backtrace corresponding to it
    for t in tasks
        try
            wait(t)
        catch e
            throw(t.exception)
        end
    end

    # Copy data for any tasks that finished before others widened columns
    oldoutcols = outcols
    outcols = outcolsref[]
    if outcols !== oldoutcols # first group
        for k in 1:length(outcols)
            outcols[k][1] = oldoutcols[k][1]
        end
    end
    for (tid, idx) in enumerate(partitions)
        if type_widened[tid]
            oldoutcols = fetch(tasks[tid])
            for k in 1:length(outcols)
                # Check whether this particular column has been widened
                if oldoutcols[k] !== outcols[k]
                    copyto!(outcols[k], first(idx), oldoutcols[k], first(idx),
                            last(idx) - first(idx) + 1)
                end
            end
        end
    end

    return outcols, colnames
end

# This needs to be in a separate function
# to work around a crash due to JuliaLang/julia#29430
if VERSION >= v"1.1.0-DEV.723"
    @inline function do_append!(do_it, col, vals)
        do_it && append!(col, vals)
        return do_it
    end
else
    @noinline function do_append!(do_it, col, vals)
        do_it && append!(col, vals)
        return do_it
    end
end

_get_col(rows::AbstractDataFrame, j::Int) = rows[!, j]
_get_col(rows::NamedTuple{<:Any, <:Tuple{Vararg{AbstractVector}}}, j::Int) = rows[j]

function append_rows!(rows, outcols::NTuple{N, AbstractVector},
                      colstart::Integer, colnames::NTuple{N, Symbol}) where N
    if !_names_match(rows, colnames)
        throw(ArgumentError("return value must have the same column names " *
                            "for all groups (got $colnames and $(propertynames(rows)))"))
    end
    @inbounds for j in colstart:length(outcols)
        col = outcols[j]
        cn = colnames[j]
        vals = _get_col(rows, j)
        S = eltype(vals)
        T = eltype(col)
        if !do_append!(S <: T || promote_type(S, T) <: T, col, vals)
            return j
        end
    end
    return nothing
end

function _combine_tables_with_first!(first::Union{AbstractDataFrame,
                                     NamedTuple{<:Any, <:Tuple{Vararg{AbstractVector}}}},
                                     outcols::NTuple{N, AbstractVector},
                                     idx::Vector{Int}, rowstart::Integer, colstart::Integer,
                                     f::Base.Callable, gd::GroupedDataFrame,
                                     incols::Union{Nothing, AbstractVector, Tuple, NamedTuple},
                                     colnames::NTuple{N, Symbol},
                                     firstmulticol::FirstColCount) where N
    len = length(gd)
    gdidx = gd.idx
    starts = gd.starts
    ends = gd.ends
    # Handle first group

    @assert _ncol(first) == N
    if !isempty(colnames) && length(gd) > 0
        j = append_rows!(first, outcols, colstart, colnames)
        @assert j === nothing # eltype is guaranteed to match
        append_const!(idx, gdidx[starts[rowstart]], _nrow(first))
    end
    # Handle remaining groups
    @inbounds for i in rowstart+1:len
        rows = wrap_table(do_call(f, gdidx, starts, ends, gd, incols, i), firstmulticol)
        _ncol(rows) == 0 && continue
        if isempty(colnames)
            newcolnames = tuple(propertynames(rows)...)
            if rows isa AbstractDataFrame
                eltys = eltype.(eachcol(rows))
            else
                eltys = map(eltype, rows)
            end
            initialcols = ntuple(i -> Tables.allocatecolumn(eltys[i], 0), _ncol(rows))
            return _combine_tables_with_first!(rows, initialcols, idx, i, 1,
                                               f, gd, incols, newcolnames, firstmulticol)
        end
        j = append_rows!(rows, outcols, 1, colnames)
        if j !== nothing # Need to widen column type
            local newcols
            let i = i, j = j, outcols=outcols, rows=rows # Workaround for julia#15276
                newcols = ntuple(length(outcols)) do k
                    S = eltype(rows isa AbstractDataFrame ? rows[!, k] : rows[k])
                    T = eltype(outcols[k])
                    U = promote_type(S, T)
                    if S <: T || U <: T
                        outcols[k]
                    else
                        copyto!(Tables.allocatecolumn(U, length(outcols[k])), outcols[k])
                    end
                end
            end
            return _combine_tables_with_first!(rows, newcols, idx, i, j,
                                               f, gd, incols, colnames, firstmulticol)
        end
        append_const!(idx, gdidx[starts[i]], _nrow(rows))
    end
    return outcols, colnames
end

function append_const!(idx::Vector{Int}, val::Int, growsize::Int)
    if growsize > 0
        oldsize = length(idx)
        newsize = oldsize + growsize
        resize!(idx, newsize)
        idx[oldsize+1:newsize] .= val
    end
end
