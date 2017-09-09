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

mutable struct UserColOrdering{T<:ColumnIndex}
    col::T
    kwargs
end

# This is exported, and lets a user define orderings for a particular column
order(col::T; kwargs...) where {T<:ColumnIndex} = UserColOrdering{T}(col, kwargs)

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

# DFPerm: defines a permutation on a particular DataFrame, using
#         a single ordering (O<:Ordering) or a list of column orderings
#         (O<:AbstractVector{Ordering}), one per DataFrame column
#
#         If a user only specifies a few columns, the DataFrame
#         contained in the DFPerm only contains those columns, and
#         the permutation induced by this ordering is used to
#         sort the original (presumably larger) DataFrame

struct DFPerm{O<:Union{Ordering, AbstractVector}, DF<:AbstractDataFrame} <: Ordering
    ord::O
    df::DF
end

function DFPerm(ords::AbstractVector{O}, df::DF) where {O<:Ordering, DF<:AbstractDataFrame}
    if length(ords) != ncol(df)
        error("DFPerm: number of column orderings does not equal the number of DataFrame columns")
    end
    DFPerm{typeof(ords), DF}(ords, df)
end

DFPerm(o::O, df::DF) where {O<:Ordering, DF<:AbstractDataFrame} = DFPerm{O,DF}(o,df)

# get ordering function for the i-th column used for ordering
col_ordering(o::DFPerm{O}, i::Int) where {O<:Ordering} = o.ord
col_ordering(o::DFPerm{V}, i::Int) where {V<:AbstractVector} = o.ord[i]

Base.@propagate_inbounds Base.getindex(o::DFPerm, i::Int, j::Int) = o.df[i, j]
Base.@propagate_inbounds Base.getindex(o::DFPerm, a::DataFrameRow, j::Int) = a[j]

function Sort.lt(o::DFPerm, a, b)
    @inbounds for i = 1:ncol(o.df)
        ord = col_ordering(o, i)
        va = o[a, i]
        vb = o[b, i]
        lt(ord, va, vb) && return true
        lt(ord, vb, va) && return false
    end
    false # a and b are equal
end

###
# Get an Ordering for a DataFrame
###

################
## Case 1: no columns requested, so sort by all columns using requested order
################
## Case 1a: single order
######
ordering(df::AbstractDataFrame, lt::Function, by::Function, rev::Bool, order::Ordering) =
    DFPerm(Order.ord(lt, by, rev, order), df)

######
## Case 1b: lt, by, rev, and order are Arrays
######
function ordering(df::AbstractDataFrame,
                  lt::AbstractVector{S}, by::AbstractVector{T},
                  rev::AbstractVector{Bool}, order::AbstractVector) where {S<:Function, T<:Function}
    if !(length(lt) == length(by) == length(rev) == length(order) == size(df,2))
        throw(ArgumentError("Orderings must be specified for all DataFrame columns"))
    end
    DFPerm([Order.ord(_lt, _by, _rev, _order) for (_lt, _by, _rev, _order) in zip(lt, by, rev, order)], df)
end

################
## Case 2:  Return a regular permutation when there's only one column
################
## Case 2a: The column is given directly
######
ordering(df::AbstractDataFrame, col::ColumnIndex, lt::Function, by::Function, rev::Bool, order::Ordering) =
    Perm(Order.ord(lt, by, rev, order), df[col])

######
## Case 2b: The column is given as a UserColOrdering
######
ordering(df::AbstractDataFrame, col_ord::UserColOrdering, lt::Function, by::Function, rev::Bool, order::Ordering) =
    Perm(ordering(col_ord, lt, by, rev, order), df[col_ord.col])

################
## Case 3:  General case: cols is an iterable of a combination of ColumnIndexes and UserColOrderings
################
## Case 3a: None of lt, by, rev, or order is an Array
######
function ordering(df::AbstractDataFrame, cols::AbstractVector, lt::Function, by::Function, rev::Bool, order::Ordering)

    if length(cols) == 0
        return ordering(df, lt, by, rev, order)
    end

    if length(cols) == 1
        return ordering(df, cols[1], lt, by, rev, order)
    end

    # Collect per-column ordering info

    ords = Ordering[]
    newcols = Int[]

    for col in cols
        push!(ords, ordering(col, lt, by, rev, order))
        push!(newcols, index(df)[(_getcol(col))])
    end

    # Simplify ordering when all orderings are the same
    if all([ords[i] == ords[1] for i = 2:length(ords)])
        return DFPerm(ords[1], df[newcols])
    end

    return DFPerm(ords, df[newcols])
end

######
# Case 3b: cols, lt, by, rev, and order are all arrays
######
function ordering(df::AbstractDataFrame, cols::AbstractVector,
                  lt::AbstractVector{S}, by::AbstractVector{T},
                  rev::AbstractVector{Bool}, order::AbstractVector) where {S<:Function, T<:Function}

    if !(length(lt) == length(by) == length(rev) == length(order))
        throw(ArgumentError("All ordering arguments must be 1 or the same length."))
    end

    if length(cols) == 0
        return ordering(df, lt, by, rev, order)
    end

    if length(lt) != length(cols)
        throw(ArgumentError("All ordering arguments must be 1 or the same length as the number of columns requested."))
    end

    if length(cols) == 1
        return ordering(df, cols[1], lt[1], by[1], rev[1], order[1])
    end

    # Collect per-column ordering info

    ords = Ordering[]
    newcols = Int[]

    for i in 1:length(cols)
        push!(ords, ordering(cols[i], lt[i], by[i], rev[i], order[i]))
        push!(newcols, index(df)[(_getcol(cols[i]))])
    end

    # Simplify ordering when all orderings are the same
    if all([ords[i] == ords[1] for i = 2:length(ords)])
        return DFPerm(ords[1], df[newcols])
    end

    return DFPerm(ords, df[newcols])
end

######
## At least one of lt, by, rev, or order is an array or tuple, so expand all to arrays
######
function ordering(df::AbstractDataFrame, cols::AbstractVector, lt, by, rev, order)
    to_array(src::AbstractVector, dims) = src
    to_array(src::Tuple, dims) = [src...]
    to_array(src, dims) = fill(src, dims)

    dims = length(cols) > 0 ? length(cols) : size(df,2)
    ordering(df, cols,
             to_array(lt, dims),
             to_array(by, dims),
             to_array(rev, dims),
             to_array(order, dims))
end

#### Convert cols from tuple to Array, if necessary
ordering(df::AbstractDataFrame, cols::Tuple, args...) = ordering(df, [cols...], args...)


###########################
# Default sorting algorithm
###########################

# TimSort is fast for data with structure, but only if the DataFrame is large enough
# TODO: 8192 is informed but somewhat arbitrary

Sort.defalg(df::AbstractDataFrame) = size(df, 1) < 8192 ? Sort.MergeSort : SortingAlgorithms.TimSort

# For DataFrames, we can choose the algorithm based on the column type and requested ordering
function Sort.defalg(df::AbstractDataFrame, ::Type{T}, o::Ordering) where T<:Real
    # If we're sorting a single numerical column in forward or reverse,
    # RadixSort will generally be the fastest stable sort
    if isbits(T) && sizeof(T) <= 8 && (o==Order.Forward || o==Order.Reverse)
        SortingAlgorithms.RadixSort
    else
        Sort.defalg(df)
    end
end
Sort.defalg(df::AbstractDataFrame,        ::Type,            o::Ordering) = Sort.defalg(df)
Sort.defalg(df::AbstractDataFrame, col    ::ColumnIndex,     o::Ordering) = Sort.defalg(df, eltype(df[col]), o)
Sort.defalg(df::AbstractDataFrame, col_ord::UserColOrdering, o::Ordering) = Sort.defalg(df, col_ord.col, o)
Sort.defalg(df::AbstractDataFrame, cols,                     o::Ordering) = Sort.defalg(df)

function Sort.defalg(df::AbstractDataFrame, o::Ordering; alg=nothing, cols=[])
    alg != nothing && return alg
    Sort.defalg(df, cols, o)
end

########################
## Actual sort functions
########################

Base.issorted(df::AbstractDataFrame; cols=Any[], lt=isless, by=identity, rev=false, order=Forward) =
    issorted(eachrow(df), ordering(df, cols, lt, by, rev, order))

# sort and sortperm functions

for s in [:(Base.sort), :(Base.sortperm)]
    @eval begin
        function $s(df::AbstractDataFrame; cols=Any[], alg=nothing,
                    lt=isless, by=identity, rev=false, order=Forward)
            if !(isa(by, Function) || eltype(by) <: Function)
                msg = "'by' must be a Function or a vector of Functions. Perhaps you wanted 'cols'."
                throw(ArgumentError(msg))
            end
            ord = ordering(df, cols, lt, by, rev, order)
            _alg = Sort.defalg(df, ord; alg=alg, cols=cols)
            $s(df, _alg, ord)
        end
    end
end

Base.sort(df::AbstractDataFrame, a::Algorithm, o::Ordering) = df[sortperm(df, a, o),:]
Base.sortperm(df::AbstractDataFrame, a::Algorithm, o::Union{Perm,DFPerm}) = sort!([1:size(df, 1);], a, o)
Base.sortperm(df::AbstractDataFrame, a::Algorithm, o::Ordering) = sortperm(df, a, DFPerm(o,df))
