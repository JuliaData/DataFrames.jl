##############################################################################
##
## Sorting
##
##############################################################################

#########################################
## Permutation & Ordering types/functions
#########################################
# Sorting in Julia works through Orderings, where each ordering is a type
# which defines a comparison function lt(o::Ord, a, b).

# UserColOrdering: user ordering of a column; this is just a convenience container
#                  which allows a user to specify column specific orderings
#                  with "order(column, rev=true,...)"

type UserColOrdering{T<:ColumnIndex}
    col::T
    kwargs
end

# This is exported, and lets a user define orderings for a particular column
order{T<:ColumnIndex}(col::T; kwargs...) = UserColOrdering{T}(col, kwargs)

# Allow getting the column even if it is not wrapped in a UserColOrdering
_getcol(o::UserColOrdering) = o.col
_getcol(x) = x

###
# Get an Ordering for a single column
###
function ordering(col_ord::UserColOrdering, lt::Function, by::Function, rev::Bool, order::Ordering)
    for (k,v) in col_ord.kwargs
        if     k == :lt;    lt    = v
        elseif k == :by;    by    = v
        elseif k == :rev;   rev   = v
        elseif k == :order; order = v
        else
            error("Unknown keyword argument: ", string(k))
        end
    end

    Order.ord(lt,by,rev,order)
end

ordering(col::ColumnIndex, lt::Function, by::Function, rev::Bool, order::Ordering) =
             Order.ord(lt,by,rev,order)

# DTPerm: defines a permutation on a particular DataTable, using
#         a single ordering (O<:Ordering) or a list of column orderings
#         (O<:AbstractVector{Ordering}), one per DataTable column
#
#         If a user only specifies a few columns, the DataTable
#         contained in the DTPerm only contains those columns, and
#         the permutation induced by this ordering is used to
#         sort the original (presumably larger) DataTable

immutable DTPerm{O<:Union{Ordering, AbstractVector}, DT<:AbstractDataTable} <: Ordering
    ord::O
    dt::DT
end

function DTPerm{O<:Ordering, DT<:AbstractDataTable}(ords::AbstractVector{O}, dt::DT)
    if length(ords) != ncol(dt)
        error("DTPerm: number of column orderings does not equal the number of DataTable columns")
    end
    DTPerm{typeof(ords), DT}(ords, dt)
end

DTPerm{O<:Ordering, DT<:AbstractDataTable}(o::O, dt::DT) = DTPerm{O,DT}(o,dt)

# get ordering function for the i-th column used for ordering
col_ordering{O<:Ordering}(o::DTPerm{O}, i::Int) = o.ord
col_ordering{V<:AbstractVector}(o::DTPerm{V}, i::Int) = o.ord[i]

Base.@propagate_inbounds Base.getindex(o::DTPerm, i::Int, j::Int) = o.dt[i, j]
Base.@propagate_inbounds Base.getindex(o::DTPerm, a::DataTableRow, j::Int) = a[j]

function Sort.lt(o::DTPerm, a, b)
    @inbounds for i = 1:ncol(o.dt)
        ord = col_ordering(o, i)
        va = o[a, i]
        vb = o[b, i]
        lt(ord, va, vb) && return true
        lt(ord, vb, va) && return false
    end
    false # a and b are equal
end

###
# Get an Ordering for a DataTable
###

################
## Case 1: no columns requested, so sort by all columns using requested order
################
## Case 1a: single order
######
ordering(dt::AbstractDataTable, lt::Function, by::Function, rev::Bool, order::Ordering) =
    DTPerm(Order.ord(lt, by, rev, order), dt)

######
## Case 1b: lt, by, rev, and order are Arrays
######
function ordering{S<:Function, T<:Function}(dt::AbstractDataTable,
                                            lt::AbstractVector{S}, by::AbstractVector{T},
                                            rev::AbstractVector{Bool}, order::AbstractVector)
    if !(length(lt) == length(by) == length(rev) == length(order) == size(dt,2))
        throw(ArgumentError("Orderings must be specified for all DataTable columns"))
    end
    DTPerm([Order.ord(_lt, _by, _rev, _order) for (_lt, _by, _rev, _order) in zip(lt, by, rev, order)], dt)
end

################
## Case 2:  Return a regular permutation when there's only one column
################
## Case 2a: The column is given directly
######
ordering(dt::AbstractDataTable, col::ColumnIndex, lt::Function, by::Function, rev::Bool, order::Ordering) =
    Perm(Order.ord(lt, by, rev, order), dt[col])

######
## Case 2b: The column is given as a UserColOrdering
######
ordering(dt::AbstractDataTable, col_ord::UserColOrdering, lt::Function, by::Function, rev::Bool, order::Ordering) =
    Perm(ordering(col_ord, lt, by, rev, order), dt[col_ord.col])

################
## Case 3:  General case: cols is an iterable of a combination of ColumnIndexes and UserColOrderings
################
## Case 3a: None of lt, by, rev, or order is an Array
######
function ordering(dt::AbstractDataTable, cols::AbstractVector, lt::Function, by::Function, rev::Bool, order::Ordering)

    if length(cols) == 0
        return ordering(dt, lt, by, rev, order)
    end

    if length(cols) == 1
        return ordering(dt, cols[1], lt, by, rev, order)
    end

    # Collect per-column ordering info

    ords = Ordering[]
    newcols = Int[]

    for col in cols
        push!(ords, ordering(col, lt, by, rev, order))
        push!(newcols, index(dt)[(_getcol(col))])
    end

    # Simplify ordering when all orderings are the same
    if all([ords[i] == ords[1] for i = 2:length(ords)])
        return DTPerm(ords[1], dt[newcols])
    end

    return DTPerm(ords, dt[newcols])
end

######
# Case 3b: cols, lt, by, rev, and order are all arrays
######
function ordering{S<:Function, T<:Function}(dt::AbstractDataTable, cols::AbstractVector,
                                            lt::AbstractVector{S}, by::AbstractVector{T},
                                            rev::AbstractVector{Bool}, order::AbstractVector)

    if !(length(lt) == length(by) == length(rev) == length(order))
        throw(ArgumentError("All ordering arguments must be 1 or the same length."))
    end

    if length(cols) == 0
        return ordering(dt, lt, by, rev, order)
    end

    if length(lt) != length(cols)
        throw(ArgumentError("All ordering arguments must be 1 or the same length as the number of columns requested."))
    end

    if length(cols) == 1
        return ordering(dt, cols[1], lt[1], by[1], rev[1], order[1])
    end

    # Collect per-column ordering info

    ords = Ordering[]
    newcols = Int[]

    for i in 1:length(cols)
        push!(ords, ordering(cols[i], lt[i], by[i], rev[i], order[i]))
        push!(newcols, index(dt)[(_getcol(cols[i]))])
    end

    # Simplify ordering when all orderings are the same
    if all([ords[i] == ords[1] for i = 2:length(ords)])
        return DTPerm(ords[1], dt[newcols])
    end

    return DTPerm(ords, dt[newcols])
end

######
## At least one of lt, by, rev, or order is an array or tuple, so expand all to arrays
######
function ordering(dt::AbstractDataTable, cols::AbstractVector, lt, by, rev, order)
    to_array(src::AbstractVector, dims) = src
    to_array(src::Tuple, dims) = [src...]
    to_array(src, dims) = fill(src, dims)

    dims = length(cols) > 0 ? length(cols) : size(dt,2)
    ordering(dt, cols,
             to_array(lt, dims),
             to_array(by, dims),
             to_array(rev, dims),
             to_array(order, dims))
end

#### Convert cols from tuple to Array, if necessary
ordering(dt::AbstractDataTable, cols::Tuple, args...) = ordering(dt, [cols...], args...)


###########################
# Default sorting algorithm
###########################

# TimSort is fast for data with structure, but only if the DataTable is large enough
# TODO: 8192 is informed but somewhat arbitrary

Sort.defalg(dt::AbstractDataTable) = size(dt, 1) < 8192 ? Sort.MergeSort : SortingAlgorithms.TimSort

# For DataTables, we can choose the algorithm based on the column type and requested ordering
function Sort.defalg{T<:Real}(dt::AbstractDataTable, ::Type{T}, o::Ordering)
    # If we're sorting a single numerical column in forward or reverse,
    # RadixSort will generally be the fastest stable sort
    if isbits(T) && sizeof(T) <= 8 && (o==Order.Forward || o==Order.Reverse)
        SortingAlgorithms.RadixSort
    else
        Sort.defalg(dt)
    end
end
Sort.defalg(dt::AbstractDataTable,        ::Type,            o::Ordering) = Sort.defalg(dt)
Sort.defalg(dt::AbstractDataTable, col    ::ColumnIndex,     o::Ordering) = Sort.defalg(dt, eltype(dt[col]), o)
Sort.defalg(dt::AbstractDataTable, col_ord::UserColOrdering, o::Ordering) = Sort.defalg(dt, col_ord.col, o)
Sort.defalg(dt::AbstractDataTable, cols,                     o::Ordering) = Sort.defalg(dt)

function Sort.defalg(dt::AbstractDataTable, o::Ordering; alg=nothing, cols=[])
    alg != nothing && return alg
    Sort.defalg(dt, cols, o)
end

########################
## Actual sort functions
########################

Base.issorted(dt::AbstractDataTable; cols=Any[], lt=isless, by=identity, rev=false, order=Forward) =
    issorted(eachrow(dt), ordering(dt, cols, lt, by, rev, order))

# sort and sortperm functions

for s in [:(Base.sort), :(Base.sortperm)]
    @eval begin
        function $s(dt::AbstractDataTable; cols=Any[], alg=nothing,
                    lt=isless, by=identity, rev=false, order=Forward)
            if !(isa(by, Function) || eltype(by) <: Function)
                msg = "'by' must be a Function or a vector of Functions. Perhaps you wanted 'cols'."
                throw(ArgumentError(msg))
            end
            ord = ordering(dt, cols, lt, by, rev, order)
            _alg = Sort.defalg(dt, ord; alg=alg, cols=cols)
            $s(dt, _alg, ord)
        end
    end
end

Base.sort(dt::AbstractDataTable, a::Algorithm, o::Ordering) = dt[sortperm(dt, a, o),:]
Base.sortperm(dt::AbstractDataTable, a::Algorithm, o::Union{Perm,DTPerm}) = sort!([1:size(dt, 1);], a, o)
Base.sortperm(dt::AbstractDataTable, a::Algorithm, o::Ordering) = sortperm(dt, a, DTPerm(o,dt))
