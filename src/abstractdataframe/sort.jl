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
#                  with "order(column, rev=true, ...)"

import SortingAlgorithms.DataStructures.FasterForward,
       SortingAlgorithms.DataStructures.FasterReverse

struct UserColOrdering{T<:ColumnIndex}
    col::T
    kwargs
end

"""
    order(col::ColumnIndex; kwargs...)

Specify sorting order for a column `col` in a data frame.
`kwargs` can be `lt`, `by`, `rev`, and `order` with values
following the rules defined in [`sort!`](@ref).

See also: [`sort!`](@ref), [`sort`](@ref)

# Examples
```jldoctest
julia> df = DataFrame(x=[-3, -1, 0, 2, 4], y=1:5)
5×2 DataFrame
 Row │ x      y
     │ Int64  Int64
─────┼──────────────
   1 │    -3      1
   2 │    -1      2
   3 │     0      3
   4 │     2      4
   5 │     4      5

julia> sort(df, order(:x, rev=true))
5×2 DataFrame
 Row │ x      y
     │ Int64  Int64
─────┼──────────────
   1 │     4      5
   2 │     2      4
   3 │     0      3
   4 │    -1      2
   5 │    -3      1

julia> sort(df, order(:x, by=abs))
5×2 DataFrame
 Row │ x      y
     │ Int64  Int64
─────┼──────────────
   1 │     0      3
   2 │    -1      2
   3 │     2      4
   4 │    -3      1
   5 │     4      5
```
"""
order(col::T; kwargs...) where {T<:ColumnIndex} = UserColOrdering{T}(col, kwargs)

# Allow getting the column even if it is not wrapped in a UserColOrdering
_getcol(o::UserColOrdering) = o.col
_getcol(x) = x

###
# Get an Ordering for a single column
###
function ordering(col_ord::UserColOrdering, lt::Function, by::Function,
                  rev::Bool, order::Ordering)
    for (k, v) in pairs(col_ord.kwargs)
        if     k == :lt;    lt    = v
        elseif k == :by;    by    = v
        elseif k == :rev;   rev   = v
        elseif k == :order; order = v
        else
            error("Unknown keyword argument: ", string(k))
        end
    end

    Order.ord(lt, by, rev, order)
end

ordering(col::ColumnIndex, lt::Function, by::Function, rev::Bool, order::Ordering) =
    Order.ord(lt, by, rev, order)

# DFPerm: defines a permutation on a particular DataFrame, using
#         a single ordering (O<:Ordering) or a list of column orderings
#         (NTuple of Ordering), one per DataFrame column
#
#         If a user only specifies a few columns, the DataFrame
#         contained in the DFPerm only contains those columns, and
#         the permutation induced by this ordering is used to
#         sort the original (presumably larger) DataFrame

struct DFPerm{O<:Union{Ordering, Tuple{Vararg{Ordering}}},
              T<:Tuple{Vararg{AbstractVector}}} <: Ordering
    ord::O
    cols::T
end

function DFPerm(ords::AbstractVector{O}, cols::T) where {O<:Ordering, T<:Tuple}
    if length(ords) != length(cols)
        error("DFPerm: number of column orderings does not equal the number of columns")
    end
    DFPerm(Tuple(ords), cols)
end

DFPerm(o::Union{Ordering, AbstractVector}, df::AbstractDataFrame) =
    DFPerm(o, ntuple(i -> df[!, i], ncol(df)))

@inline col_ordering(o::Ordering) = o
@inline ord_tail(o::Ordering) = o
@inline col_ordering(o::Tuple{Vararg{Ordering}}) = @inbounds o[1]
@inline ord_tail(o::Tuple{Vararg{Ordering}}) = Base.tail(o)

Sort.lt(o::DFPerm{<:Any, Tuple{}}, a, b) = false

function Sort.lt(o::DFPerm{<:Any, <:Tuple}, a, b)
    ord = o.ord
    cols = o.cols
    # if there are too many columns fall back to type unstable mode to avoid high compilation cost
    # it is expected that in practice users sort data frames on only few columns
    length(cols) > 16 && return unstable_lt(ord, cols, a, b)

    @inbounds begin
        ord1 = col_ordering(ord)
        col = first(cols)
        va = col[a]
        vb = col[b]
        lt(ord1, va, vb) && return true
        lt(ord1, vb, va) && return false
    end
    return Sort.lt(DFPerm(ord_tail(ord), Base.tail(cols)), a, b)
end

# get ordering function for the i-th column used for ordering
col_ordering(o::Ordering, ::Int) = o
col_ordering(o::Tuple{Vararg{Ordering}}, i::Int) = @inbounds o[i]

function unstable_lt(ord::Union{Ordering, Tuple{Vararg{Ordering}}},
                     cols::Tuple{Vararg{AbstractVector}}, a, b)
    for i in 1:length(cols)
        ordi = col_ordering(ord, i)
        @inbounds coli = cols[i]
        @inbounds va = coli[a]
        @inbounds vb = coli[b]
        lt(ordi, va, vb) && return true
        lt(ordi, vb, va) && return false
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
                  rev::AbstractVector{Bool},
                  order::AbstractVector) where {S<:Function, T<:Function}
    if !(length(lt) == length(by) == length(rev) == length(order) == size(df, 2))
        throw(ArgumentError("Orderings must be specified for all DataFrame columns"))
    end
    DFPerm([Order.ord(_lt, _by, _rev, _order) for
            (_lt, _by, _rev, _order) in zip(lt, by, rev, order)], df)
end

################
## Case 2:  Return a regular permutation when there's only one column
################
## Case 2a: The column is given directly
######
ordering(df::AbstractDataFrame, col::ColumnIndex, lt::Function, by::Function,
         rev::Bool, order::Ordering) =
    Perm(Order.ord(lt, by, rev, order), df[!, col])

######
## Case 2b: The column is given as a UserColOrdering
######
ordering(df::AbstractDataFrame, col_ord::UserColOrdering, lt::Function, by::Function,
         rev::Bool, order::Ordering) =
    Perm(ordering(col_ord, lt, by, rev, order), df[!, col_ord.col])

################
## Case 3:  General case: cols is an iterable of a combination of ColumnIndexes and UserColOrderings
################
## Case 3a: None of lt, by, rev, or order is an Array
######
function ordering(df::AbstractDataFrame, cols::AbstractVector, lt::Function,
                  by::Function, rev::Bool, order::Ordering)

    if length(cols) == 0
        if ncol(df) > 0
            Base.depwarn("When empty column selector is passed ordering is done on all columns. " *
                        "This behavior is deprecated and will change in the future.", :ordering)
        end
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
        return DFPerm(ords[1], df[!, newcols])
    end

    return DFPerm(ords, df[!, newcols])
end

######
# Case 3b: cols, lt, by, rev, and order are all arrays
######
function ordering(df::AbstractDataFrame, cols::AbstractVector, lt::AbstractVector{S},
                  by::AbstractVector{T}, rev::AbstractVector{Bool},
                  order::AbstractVector) where {S<:Function, T<:Function}

    if !(length(lt) == length(by) == length(rev) == length(order))
        throw(ArgumentError("All ordering arguments must be 1 or the same length."))
    end

    if length(cols) == 0
        return ordering(df, lt, by, rev, order)
    end

    if length(lt) != length(cols)
        throw(ArgumentError("All ordering arguments must be 1 or the same length " *
                            "as the number of columns requested."))
    end

    if length(cols) == 1
        return ordering(df, only(cols), only(lt), only(by), only(rev), only(order))
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
        return DFPerm(ords[1], df[!, newcols])
    end

    return DFPerm(ords, df[!, newcols])
end

######
## At least one of lt, by, rev, or order is an array or tuple, so expand all to arrays
######
function ordering(df::AbstractDataFrame, cols::AbstractVector, lt, by, rev, order)
    to_array(src::AbstractVector, dims) = src
    to_array(src::Tuple, dims) = [src...]
    to_array(src, dims) = fill(src, dims)

    dims = length(cols) > 0 ? length(cols) : size(df, 2)
    ordering(df, cols,
             to_array(lt, dims),
             to_array(by, dims),
             to_array(rev, dims),
             to_array(order, dims))
end

# an explicit error is thrown as Tuple was supported in the past
ordering(df::AbstractDataFrame, cols::Tuple, args...) =
    throw(ArgumentError("Passing a tuple $cols of column selectors when sorting data " *
                        "frame is not supported. Pass a vector $([cols...]) instead."))

###########################
# Default sorting algorithm
###########################

# TimSort is fast for data with structure, but only if the DataFrame is large enough
# TODO: 8192 is informed but somewhat arbitrary

Sort.defalg(df::AbstractDataFrame) =
    size(df, 1) < 8192 ? Sort.MergeSort : SortingAlgorithms.TimSort

# For DataFrames, we can choose the algorithm based on the column type and requested ordering
function Sort.defalg(df::AbstractDataFrame, ::Type{T}, o::Ordering) where T<:Real
    # If we're sorting a single numerical column in forward or reverse,
    # RadixSort will generally be the fastest stable sort
    if isbitstype(T) && sizeof(T) <= 8 && (o==Order.Forward || o==Order.Reverse)
        SortingAlgorithms.RadixSort
    else
        Sort.defalg(df)
    end
end

Sort.defalg(df::AbstractDataFrame, ::Type, o::Ordering) = Sort.defalg(df)
Sort.defalg(df::AbstractDataFrame, col::ColumnIndex, o::Ordering) =
    Sort.defalg(df, eltype(df[!, col]), o)
Sort.defalg(df::AbstractDataFrame, col_ord::UserColOrdering, o::Ordering) =
    Sort.defalg(df, col_ord.col, o)
Sort.defalg(df::AbstractDataFrame, cols, o::Ordering) = Sort.defalg(df)
Sort.defalg(df::AbstractDataFrame, o::Ordering; alg=nothing, cols=[]) =
    alg !== nothing ? alg : Sort.defalg(df, cols, o)

########################
## Actual sort functions
########################

const SORT_ARGUMENTS =
"""
If `rev` is `true`, reverse sorting is performed. To enable reverse sorting only
for some columns, pass `order(c, rev=true)` in `cols`, with `c` the
corresponding column index (see example below).

Since having repeated elements makes multiple sorting orders valid, the
`checkunique` keyword allows for the situation to be caught. If `checkunique` is
`true` and duplicate elements are found an error will be thrown. The use of the
`checkunique` keyword is only supported when neither the `by` nor the `lt`
keywords are being used. Similarly, the use of `order(...)` clauses that specify
either `by` or `lt` are not supported, but specifying `rev` by itself is
allowed.

The `by` keyword allows providing a function that will be applied to each
cell before comparison; the `lt` keyword allows providing a custom "less
than" function. If both `by` and `lt` are specified, the `lt` function is
applied to the result of the `by` function.

Keyword arguments specifying sorting order (`rev`, `lt` or `by`) can either be
a single value, or a vector of length equal to the number of columns the
operation is performed on. When a single value is passed, it applies to all
columns. When a vector is passed, each entry applies to the column in the
corresponding position in `cols`.
"""

"""
    issorted(df::AbstractDataFrame, cols=All();
             lt::Union{Function, AbstractVector{<:Function}}=isless,
             by::Union{Function, AbstractVector{<:Function}}=identity,
             rev::Union{Bool, AbstractVector{Bool}}=false,
             order::Union{Ordering, AbstractVector{<:Ordering}}=Forward,
             checkunique::Bool=false)

Test whether data frame `df` sorted by column(s) `cols`. Checking against
multiple columns is done lexicographically.

`cols` can be any column selector ($COLUMNINDEX_STR; $MULTICOLUMNINDEX_STR). If
`cols` selects no columns, check whether `df` is sorted on all columns (this
behaviour is deprecated and will change in future versions).

$SORT_ARGUMENTS

# Examples
```jldoctest
julia> df = DataFrame(a=[1, 2, 3, 4], b=[4, 3, 2, 1])
4×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      4
   2 │     2      3
   3 │     3      2
   4 │     4      1

julia> issorted(df)
true

julia> issorted(df, :a)
true

julia> issorted(df, :b)
false

julia> issorted(df, :b, rev=true)
true
```
"""
function Base.issorted(df::AbstractDataFrame, cols=All();
                       lt::Union{Function, AbstractVector{<:Function}}=isless,
                       by::Union{Function, AbstractVector{<:Function}}=identity,
                       rev::Union{Bool, AbstractVector{Bool}}=false,
                       order::Union{Ordering, AbstractVector{<:Ordering}}=Forward,
                       checkunique::Bool=false)
    to_scalar(x::AbstractVector) = only(x)
    to_scalar(x::Any) = x

    # exclude AbstractVector as in that case cols can contain order(...) clauses
    if cols isa MultiColumnIndex && !(cols isa AbstractVector)
        cols = index(df)[cols]
    end
    checkunique && _perform_uniqueness_checks(df, cols, lt, by, order)
    if cols isa ColumnIndex
        return issorted(df[!, cols], lt=to_scalar(lt), by=to_scalar(by),
                        rev=to_scalar(rev), order=to_scalar(order))
    elseif cols isa AbstractVector{<:ColumnIndex} && length(cols) == 1
        return issorted(df[!, cols[1]], lt=to_scalar(lt), by=to_scalar(by),
                        rev=to_scalar(rev), order=to_scalar(order))
    else
        return issorted(1:nrow(df), ordering(df, cols, lt, by, rev, order))
    end
end

Base.issorted(::AbstractDataFrame, ::Base.Order.Ordering) =
    throw(ArgumentError("second positional argument of `issorted` on " *
                        "a data frame must be a column selector"))

"""
    sort(df::AbstractDataFrame, cols=All();
         alg::Union{Algorithm, Nothing}=nothing,
         lt::Union{Function, AbstractVector{<:Function}}=isless,
         by::Union{Function, AbstractVector{<:Function}}=identity,
         rev::Union{Bool, AbstractVector{Bool}}=false,
         order::Union{Ordering, AbstractVector{<:Ordering}}=Forward,
         view::Bool=false,
         checkunique::Bool=false)

Return a data frame containing the rows in `df` sorted by column(s) `cols`.
Sorting on multiple columns is done lexicographically.

`cols` can be any column selector ($COLUMNINDEX_STR; $MULTICOLUMNINDEX_STR).
If `cols` selects no columns, sort `df` on all columns
(this behaviour is deprecated and will change in future versions).

$SORT_ARGUMENTS

If `alg` is `nothing` (the default), the most appropriate algorithm is
chosen automatically among `TimSort`, `MergeSort` and `RadixSort` depending
on the type of the sorting columns and on the number of rows in `df`.

If `view=false` a freshly allocated `DataFrame` is returned.
If `view=true` then a `SubDataFrame` view into `df` is returned.

$METADATA_FIXED

# Examples
```jldoctest
julia> df = DataFrame(x=[3, 1, 2, 1], y=["b", "c", "a", "b"])
4×2 DataFrame
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     3  b
   2 │     1  c
   3 │     2  a
   4 │     1  b

julia> sort(df, :x)
4×2 DataFrame
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     1  c
   2 │     1  b
   3 │     2  a
   4 │     3  b

julia> sort(df, [:x, :y])
4×2 DataFrame
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     1  b
   2 │     1  c
   3 │     2  a
   4 │     3  b

julia> sort(df, [:x, :y], rev=true)
4×2 DataFrame
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     3  b
   2 │     2  a
   3 │     1  c
   4 │     1  b

julia> sort(df, [:x, order(:y, rev=true)])
4×2 DataFrame
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     1  c
   2 │     1  b
   3 │     2  a
   4 │     3  b
```
"""
@inline function Base.sort(df::AbstractDataFrame, cols=All();
                           alg::Union{Algorithm, Nothing}=nothing,
                           lt::Union{Function, AbstractVector{<:Function}}=isless,
                           by::Union{Function, AbstractVector{<:Function}}=identity,
                           rev::Union{Bool, AbstractVector{Bool}}=false,
                           order::Union{Ordering, AbstractVector{<:Ordering}}=Forward,
                           view::Bool=false,
                           checkunique::Bool=false)
    rowidxs = sortperm(df, cols, alg=alg, lt=lt, by=by, rev=rev, order=order,
                       checkunique=checkunique)
    return view ? Base.view(df, rowidxs, :) : df[rowidxs, :]
end

"""
    sortperm(df::AbstractDataFrame, cols=All();
             alg::Union{Algorithm, Nothing}=nothing,
             lt::Union{Function, AbstractVector{<:Function}}=isless,
             by::Union{Function, AbstractVector{<:Function}}=identity,
             rev::Union{Bool, AbstractVector{Bool}}=false,
             order::Union{Ordering, AbstractVector{<:Ordering}}=Forward,
             checkunique::Bool=false)

Return a permutation vector of row indices of data frame `df` that puts them in
sorted order according to column(s) `cols`.
Order on multiple columns is computed lexicographically.

`cols` can be any column selector ($COLUMNINDEX_STR; $MULTICOLUMNINDEX_STR).
If `cols` selects no columns, return permutation vector based on sorting all columns
(this behaviour is deprecated and will change in future versions).

$SORT_ARGUMENTS

If `alg` is `nothing` (the default), the most appropriate algorithm is
chosen automatically among `TimSort`, `MergeSort` and `RadixSort` depending
on the type of the sorting columns and on the number of rows in `df`.

# Examples
```jldoctest
julia> df = DataFrame(x=[3, 1, 2, 1], y=["b", "c", "a", "b"])
4×2 DataFrame
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     3  b
   2 │     1  c
   3 │     2  a
   4 │     1  b

julia> sortperm(df, :x)
4-element Vector{Int64}:
 2
 4
 3
 1

julia> sortperm(df, [:x, :y])
4-element Vector{Int64}:
 4
 2
 3
 1

julia> sortperm(df, [:x, :y], rev=true)
4-element Vector{Int64}:
 1
 3
 2
 4

julia> sortperm(df, [:x, order(:y, rev=true)])
4-element Vector{Int64}:
 2
 4
 3
 1
```
"""
function Base.sortperm(df::AbstractDataFrame, cols=All();
                       alg::Union{Algorithm, Nothing}=nothing,
                       lt::Union{Function, AbstractVector{<:Function}}=isless,
                       by::Union{Function, AbstractVector{<:Function}}=identity,
                       rev::Union{Bool, AbstractVector{Bool}}=false,
                       order::Union{Ordering, AbstractVector{<:Ordering}}=Forward,
                       checkunique::Bool=false)
    # exclude AbstractVector as in that case cols can contain order(...) clauses
    if cols isa MultiColumnIndex && !(cols isa AbstractVector)
        cols = index(df)[cols]
    end
    ord = ordering(df, cols, lt, by, rev, order)
    _alg = Sort.defalg(df, ord; alg=alg, cols=cols)
    checkunique && _perform_uniqueness_checks(df, cols, lt, by, order)
    return _sortperm(df, _alg, ord)
end

_sortperm(df::AbstractDataFrame, a::Algorithm, o::Union{Perm, DFPerm}) =
    sort!([1:size(df, 1);], a, o)
_sortperm(df::AbstractDataFrame, a::Algorithm, o::Ordering) =
    sortperm(df, a, DFPerm(o, df))


"""
    sort!(df::AbstractDataFrame, cols=All();
          alg::Union{Algorithm, Nothing}=nothing,
          lt::Union{Function, AbstractVector{<:Function}}=isless,
          by::Union{Function, AbstractVector{<:Function}}=identity,
          rev::Union{Bool, AbstractVector{Bool}}=false,
          order::Union{Ordering, AbstractVector{<:Ordering}}=Forward,
          checkunique::Bool=false)

Sort data frame `df` by column(s) `cols` by permuting its rows in-place.
Sorting on multiple columns is done lexicographicallly.

`cols` can be any column selector ($COLUMNINDEX_STR; $MULTICOLUMNINDEX_STR). If
`cols` selects no columns, sort `df` on all columns (this behaviour is
deprecated and will change in future versions).

$SORT_ARGUMENTS

If `alg` is `nothing` (the default), the most appropriate algorithm is chosen
automatically among `TimSort`, `MergeSort` and `RadixSort` depending on the type
of the sorting columns and on the number of rows in `df`.

`sort!` will produce a correct result even if some columns of passed data frame
are identical (checked with `===`). Otherwise, if two columns share some part of
memory but are not identical (e.g. are different views of the same parent
vector) then `sort!` result might be incorrect.

$METADATA_FIXED
Metadata having other styles is dropped (from parent data frame when `df` is a `SubDataFrame`).

# Examples
```jldoctest
julia> df = DataFrame(x=[3, 1, 2, 1], y=["b", "c", "a", "b"])
4×2 DataFrame
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     3  b
   2 │     1  c
   3 │     2  a
   4 │     1  b

julia> sort!(df, :x)
4×2 DataFrame
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     1  c
   2 │     1  b
   3 │     2  a
   4 │     3  b

julia> sort!(df, [:x, :y])
4×2 DataFrame
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     1  b
   2 │     1  c
   3 │     2  a
   4 │     3  b

julia> sort!(df, [:x, :y], rev=true)
4×2 DataFrame
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     3  b
   2 │     2  a
   3 │     1  c
   4 │     1  b

julia> sort!(df, [:x, order(:y, rev=true)])
4×2 DataFrame
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     1  c
   2 │     1  b
   3 │     2  a
   4 │     3  b
```
"""
function Base.sort!(df::AbstractDataFrame, cols=All();
                    alg::Union{Algorithm, Nothing}=nothing,
                    lt::Union{Function, AbstractVector{<:Function}}=isless,
                    by::Union{Function, AbstractVector{<:Function}}=identity,
                    rev::Union{Bool, AbstractVector{Bool}}=false,
                    order::Union{Ordering, AbstractVector{<:Ordering}}=Forward,
                    checkunique::Bool=false)

    # exclude AbstractVector as in that case cols can contain order(...) clauses
    if cols isa MultiColumnIndex && !(cols isa AbstractVector)
        cols = index(df)[cols]
    end
    ord = ordering(df, cols, lt, by, rev, order)
    _alg = Sort.defalg(df, ord; alg=alg, cols=cols)
    checkunique && _perform_uniqueness_checks(df, cols, lt, by, order)
    return sort!(df, _alg, ord)
end

# this sort! method does not support uniqueness checks since they can't be carried out
# without knowledge of which columns are to be sorted.
function Base.sort!(df::AbstractDataFrame, a::Base.Sort.Algorithm,
                    o::Base.Sort.Ordering)
    permute!(df, _sortperm(df, a, o))
end

# Functions to verify whether an Ordering has user-defined `by` or `lt` functions
# The complexity checks exploit `Base.Order`'s way of constructing `Order` objects.
# `DirectOrdering` is by definition an order that uses `isless` and `identity` as its
# `lt` and `by` functions, so they are not complex. The `By` and `Lt` types have
# attributes storing the defined `by` and `lt` functions respectively, simple identity
# checks are enough. `ReverseOrdering`s wrap another type of ordering, so we
# perform the check on the wrapped type.
is_complex(o::Union{DirectOrdering, FasterForward, FasterReverse}) = false
is_complex(o::By) = o.by !== identity
is_complex(o::Lt) = o.lt !== isless
is_complex(o::ReverseOrdering) = is_complex(o.fwd)
is_complex(o::Perm) = is_complex(o.order)

function is_complex(o::DFPerm)
    if o.ord isa Ordering
        return is_complex(o.ord)
    elseif o.ord isa Tuple
        return any(is_complex(ordering) for ordering in o.ord)
    end
    throw(ArgumentError("unsupported ord type"))
end

function is_complex(o::Ordering)
    throw(ArgumentError("The use of the keyword `checkunique` is currently " *
                        "not supported with Ordering type $(typeof(o))"))
end

function is_complex(o::UserColOrdering)
    has_lt = haskey(o.kwargs, :lt)
    has_by = haskey(o.kwargs, :by)
    if !has_lt && !has_by
        return false
    elseif has_lt && !has_by
        return o.kwargs[:lt] !== isless
    elseif has_by && !has_lt
        return o.kwargs[:by] !== identity
    else
        @assert has_lt && has_by
        return o.kwargs[:by] !== identity || o.kwargs[:lt] !== isless
    end
end

# Internal function that aids in uniqueness checks
# Converts column selectors to indices and checks necessary conditions for uniqueness
function _perform_uniqueness_checks(df::AbstractDataFrame, cols,
                                    lt::Union{Function, AbstractVector{<:Function}},
                                    by::Union{Function, AbstractVector{<:Function}},
                                    order::Union{Ordering, AbstractVector{<:Ordering}})

    if !(lt === isless && by === identity)
        throw(ArgumentError("Passing either lt or by along with checkunique=" *
                            "true is not supported."))
    end

    # Validating the order argument
    if order isa Ordering
        order = [order]
    end
    for o in order
        if is_complex(o)
            throw(ArgumentError("Using either lt or by functions through the " *
                                "order keyword argument simultaneously with " *
                                "checkunique=true is not supported."))
        end
    end

    # Easiest case, cols contains column indexes already
    if cols isa AbstractVector{<:ColumnIndex}
        by_or_lt_set = false
        col_idxs = cols
    # Second easiest, multicol index (no vector with orders clauses mixed in)
    elseif (cols isa MultiColumnIndex && !(cols isa AbstractVector)) || cols isa ColumnIndex
        by_or_lt_set = false
        col_idxs = index(df)[cols]
    elseif cols isa UserColOrdering
        by_or_lt_set = is_complex(cols)
        col_idxs = [index(df)[_getcol(cols)]]
    # Mix of ColOrdering and other ColumnSelectors
    else
        @assert cols isa AbstractVector
        newcols = Int[]
        by_or_lt_set = false
        for col in cols
            if col isa UserColOrdering
                by_or_lt_set = is_complex(col) || by_or_lt_set
            end

            push!(newcols, index(df)[_getcol(col)])
        end
        col_idxs = newcols
    end
    if by_or_lt_set
        throw(ArgumentError("Order clauses with either by or lt set in combination " *
                            "with checkunique=true are not supported"))
    end
    allunique(df, col_idxs) ||
        throw(ArgumentError("Non-unique elements found. Multiple orders are valid."))
end