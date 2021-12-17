"""
    SubDataFrame{<:AbstractDataFrame, <:AbstractIndex, <:AbstractVector{Int}} <: AbstractDataFrame

A view of an `AbstractDataFrame`. It is returned by a call to the `view` function
on an `AbstractDataFrame` if a collections of rows and columns are specified.

A `SubDataFrame` is an `AbstractDataFrame`, so expect that most
DataFrame functions should work. Such methods include `describe`,
`summary`, `nrow`, `size`, `by`, `stack`, and `join`.

If the selection of columns in a parent data frame is passed as `:` (a colon)
then `SubDataFrame` will always have all columns from the parent,
even if they are added or removed after its creation.

# Examples
```jldoctest
julia> df = DataFrame(a=repeat([1, 2, 3, 4], outer=[2]),
                      b=repeat([2, 1], outer=[4]),
                      c=1:8)
8×3 DataFrame
 Row │ a      b      c
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      2      1
   2 │     2      1      2
   3 │     3      2      3
   4 │     4      1      4
   5 │     1      2      5
   6 │     2      1      6
   7 │     3      2      7
   8 │     4      1      8

julia> sdf1 = view(df, :, 2:3) # column subsetting
8×2 SubDataFrame
 Row │ b      c
     │ Int64  Int64
─────┼──────────────
   1 │     2      1
   2 │     1      2
   3 │     2      3
   4 │     1      4
   5 │     2      5
   6 │     1      6
   7 │     2      7
   8 │     1      8

julia> sdf2 = @view df[end:-1:1, [1, 3]]  # row and column subsetting
8×2 SubDataFrame
 Row │ a      c
     │ Int64  Int64
─────┼──────────────
   1 │     4      8
   2 │     3      7
   3 │     2      6
   4 │     1      5
   5 │     4      4
   6 │     3      3
   7 │     2      2
   8 │     1      1

julia> sdf3 = groupby(df, :a)[1]  # indexing a GroupedDataFrame returns a SubDataFrame
2×3 SubDataFrame
 Row │ a      b      c
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      2      1
   2 │     1      2      5
```
"""
struct SubDataFrame{D<:AbstractDataFrame, S<:AbstractIndex, T<:AbstractVector{Int}} <: AbstractDataFrame
    parent::D
    colindex::S
    rows::T # maps from subdf row indexes to parent row indexes
end

Base.@propagate_inbounds function SubDataFrame(parent::DataFrame, rows::AbstractVector{Int}, cols)
    @boundscheck if !checkindex(Bool, axes(parent, 1), rows)
        throw(BoundsError(parent, (rows, cols)))
    end
    SubDataFrame(parent, SubIndex(index(parent), cols), rows)
end
Base.@propagate_inbounds SubDataFrame(parent::DataFrame, ::Colon, cols) =
    SubDataFrame(parent, axes(parent, 1), cols)
@inline SubDataFrame(parent::DataFrame, row::Integer, cols) =
    throw(ArgumentError("invalid row index: $row of type $(typeof(row))"))

Base.@propagate_inbounds function SubDataFrame(parent::DataFrame, rows::AbstractVector{<:Integer}, cols)
    if any(x -> x isa Bool, rows)
        throw(ArgumentError("invalid row index of type `Bool`"))
    end
    return SubDataFrame(parent, convert(Vector{Int}, rows), cols)
end

Base.@propagate_inbounds function SubDataFrame(parent::DataFrame, rows::AbstractVector{Bool}, cols)
    if length(rows) != nrow(parent)
        throw(ArgumentError("invalid length of `AbstractVector{Bool}` row index " *
                            "(got $(length(rows)), expected $(nrow(parent)))"))
    end
    return SubDataFrame(parent, _findall(rows), cols)
end

Base.@propagate_inbounds function SubDataFrame(parent::DataFrame, rows::AbstractVector, cols)
    if !all(x -> (x isa Integer) && !(x isa Bool), rows)
        throw(ArgumentError("only `Integer` indices are accepted in `rows`"))
    end
    return SubDataFrame(parent, convert(Vector{Int}, rows), cols)
end

Base.@propagate_inbounds SubDataFrame(sdf::SubDataFrame, rowind, cols) =
    SubDataFrame(parent(sdf), rows(sdf)[rowind], parentcols(index(sdf), cols))
Base.@propagate_inbounds SubDataFrame(sdf::SubDataFrame, rowind::Bool, cols) =
    throw(ArgumentError("invalid row index of type Bool"))
Base.@propagate_inbounds SubDataFrame(sdf::SubDataFrame, rowind, ::Colon) =
    if index(sdf) isa Index # sdf was created using : as row selector
        SubDataFrame(parent(sdf), rows(sdf)[rowind], :)
    else
        SubDataFrame(parent(sdf), rows(sdf)[rowind], parentcols(index(sdf), :))
    end
Base.@propagate_inbounds SubDataFrame(sdf::SubDataFrame, rowind::Bool, ::Colon) =
    throw(ArgumentError("invalid row index of type Bool"))
Base.@propagate_inbounds SubDataFrame(sdf::SubDataFrame, ::Colon, cols) =
    SubDataFrame(parent(sdf), rows(sdf), parentcols(index(sdf), cols))
@inline SubDataFrame(sdf::SubDataFrame, ::Colon, ::Colon) = sdf

rows(sdf::SubDataFrame) = getfield(sdf, :rows)
Base.parent(sdf::SubDataFrame) = getfield(sdf, :parent)
Base.parentindices(sdf::SubDataFrame) = (rows(sdf), parentcols(index(sdf)))

Base.@propagate_inbounds Base.view(adf::AbstractDataFrame, rowinds, colind::ColumnIndex) =
    view(adf[!, colind], rowinds)
Base.@propagate_inbounds Base.view(adf::AbstractDataFrame, ::typeof(!), colind::ColumnIndex) =
    view(adf[!, colind], :)
@inline Base.view(adf::AbstractDataFrame, rowinds, colind::Bool) =
    throw(ArgumentError("invalid column index $colind of type `Bool`"))
Base.@propagate_inbounds Base.view(adf::AbstractDataFrame, rowinds,
                                   colinds::MultiColumnIndex) =
    SubDataFrame(adf, rowinds, colinds)
Base.@propagate_inbounds Base.view(adf::AbstractDataFrame, rowinds::typeof(!),
                                   colinds::MultiColumnIndex) =
    SubDataFrame(adf, :, colinds)
Base.@propagate_inbounds Base.view(adf::AbstractDataFrame, rowinds::Not,
                                   colinds::MultiColumnIndex) =
    SubDataFrame(adf, axes(adf, 1)[rowinds], colinds)

##############################################################################
##
## AbstractDataFrame interface
##
##############################################################################

index(sdf::SubDataFrame) = getfield(sdf, :colindex)

nrow(sdf::SubDataFrame) = ncol(sdf) > 0 ? length(rows(sdf))::Int : 0
ncol(sdf::SubDataFrame) = length(index(sdf))

Base.@propagate_inbounds Base.getindex(sdf::SubDataFrame, rowind::Integer, colind::ColumnIndex) =
    parent(sdf)[rows(sdf)[rowind], parentcols(index(sdf), colind)]
Base.@propagate_inbounds Base.getindex(sdf::SubDataFrame, rowind::Bool, colind::ColumnIndex) =
    throw(ArgumentError("invalid row index of type Bool"))

Base.@propagate_inbounds Base.getindex(sdf::SubDataFrame, rowinds::Union{AbstractVector, Not},
                                       colind::ColumnIndex) =
    parent(sdf)[rows(sdf)[rowinds], parentcols(index(sdf), colind)]
Base.@propagate_inbounds Base.getindex(sdf::SubDataFrame, ::Colon, colind::ColumnIndex) =
    parent(sdf)[rows(sdf), parentcols(index(sdf), colind)]
Base.@propagate_inbounds Base.getindex(sdf::SubDataFrame, ::typeof(!), colind::ColumnIndex) =
    view(parent(sdf), rows(sdf), parentcols(index(sdf), colind))

Base.@propagate_inbounds Base.getindex(sdf::SubDataFrame, rowinds::Union{AbstractVector, Not},
                                       colinds::MultiColumnIndex) =
    parent(sdf)[rows(sdf)[rowinds], parentcols(index(sdf), colinds)]
Base.@propagate_inbounds Base.getindex(sdf::SubDataFrame, ::Colon,
                                       colinds::MultiColumnIndex) =
    parent(sdf)[rows(sdf), parentcols(index(sdf), colinds)]
Base.@propagate_inbounds Base.getindex(df::SubDataFrame, row_ind::typeof(!),
                                       col_inds::MultiColumnIndex) =
    select(df, index(df)[col_inds], copycols=false)

Base.@propagate_inbounds function Base.setindex!(sdf::SubDataFrame, val::Any, idx::CartesianIndex{2})
    return setindex!(sdf, val, idx[1], idx[2])
end

Base.@propagate_inbounds function Base.setindex!(sdf::SubDataFrame, val::Any, ::Colon, colinds::Any)
    if colinds isa SymbolOrString && columnindex(sdf, colinds) == 0
        if !is_column_insertion_allowed(sdf)
            throw(ArgumentError("creating new columns in a SubDataFrame that subsets " *
                                "columns of its parent data frame is disallowed"))
        end
        if !(val isa AbstractVector && nrow(sdf) == length(val))
            throw(ArgumentError("Assigned value must be a vector with length " *
                                "equal to number of rows in the SubDataFrame"))
        end
        T = eltype(val)
        newcol = similar(val, Union{T, Missing}, nrow(parent(sdf)))
        fill!(newcol, missing)
        newcol[rows(sdf)] = val
        parent(sdf)[!, colinds] = newcol
    else
        parent(sdf)[rows(sdf), parentcols(index(sdf), colinds)] = val
    end
    return sdf
end

# TODO: in the future, when refactoring source code
# (presumably when we would first define all the types that the package provides
#  and then define methods for them)
# consider merging SubDataFrame and DataFrame setindex! methods

function Base.setindex!(sdf::SubDataFrame, v::AbstractVector,
                        ::typeof(!), col_ind::ColumnIndex)
    if col_ind isa Union{Signed, Unsigned} && !(1 <= col_ind <= ncol(sdf))
        throw(ArgumentError("Cannot assign to non-existent column: $col_ind"))
    end
    if col_ind isa SymbolOrString && columnindex(sdf, col_ind) == 0
        if !is_column_insertion_allowed(sdf)
            throw(ArgumentError("creating new columns in a SubDataFrame that subsets " *
                                "columns of its parent data frame is disallowed"))
        end
        sdf[:, col_ind] = v
    else
        pdf = parent(sdf)
        p_col_ind = parentcols(index(sdf), col_ind)
        old_col = pdf[!, p_col_ind]
        T = eltype(old_col)
        S = eltype(v)
        newcol = similar(old_col, promote_type(T, S), length(old_col))
        newcol .= old_col
        newcol[rows(sdf)] = v
        pdf[!, p_col_ind] = newcol
    end
    return sdf
end

for T in MULTICOLUMNINDEX_TUPLE
    @eval function Base.setindex!(sdf::SubDataFrame,
                                  new_df::AbstractDataFrame,
                                  row_inds::typeof(!),
                                  col_inds::$T)
        idxs = index(sdf)[col_inds]
        if view(_names(sdf), idxs) != _names(new_df)
            throw(ArgumentError("Column names in source and target data frames do not match"))
        end
        for (j, col) in enumerate(idxs)
            sdf[!, col] = new_df[!, j]
        end
        return sdf
    end

    @eval function Base.setindex!(sdf::SubDataFrame,
                                  mx::AbstractMatrix,
                                  row_inds::typeof(!),
                                  col_inds::$T)
        idxs = index(sdf)[col_inds]
        if size(mx, 2) != length(idxs)
            throw(DimensionMismatch("number of selected columns ($(length(idxs))) " *
                                    "and number of columns in " *
                                    "matrix ($(size(mx, 2))) do not match"))
        end
        for (j, col) in enumerate(idxs)
            sdf[!, col] = view(mx, :, j)
        end
        return sdf
    end
end

Base.@propagate_inbounds function Base.setindex!(sdf::SubDataFrame, val::Any, rowinds::Any, colinds::Any)
    parent(sdf)[rows(sdf)[rowinds], parentcols(index(sdf), colinds)] = val
    return sdf
end

Base.@propagate_inbounds Base.setindex!(sdf::SubDataFrame, val::Any, rowinds::Bool, colinds::Any) =
    throw(ArgumentError("invalid row index of type Bool"))

Base.setproperty!(df::SubDataFrame, col_ind::Symbol, v::AbstractVector) =
    (df[!, col_ind] = v)
Base.setproperty!(df::SubDataFrame, col_ind::AbstractString, v::AbstractVector) =
    (df[!, col_ind] = v)
Base.setproperty!(::SubDataFrame, col_ind::Symbol, v::Any) =
    throw(ArgumentError("It is only allowed to pass a vector as a column of a SubDataFrame. " *
                        "Instead use `df[!, col_ind] .= v` if you want to use broadcasting."))
Base.setproperty!(::SubDataFrame, col_ind::AbstractString, v::Any) =
    throw(ArgumentError("It is only allowed to pass a vector as a column of a SubDataFrame. " *
                        "Instead use `df[!, col_ind] .= v` if you want to use broadcasting."))

##############################################################################
##
## Miscellaneous
##
##############################################################################

Base.copy(sdf::SubDataFrame) = parent(sdf)[rows(sdf), parentcols(index(sdf), :)]

Base.deleteat!(df::SubDataFrame, ind) =
    throw(ArgumentError("SubDataFrame does not support deleting rows"))

function DataFrame(sdf::SubDataFrame; copycols::Bool=true)
    if copycols
        sdf[:, :]
    else
        DataFrame(collect(eachcol(sdf)), _names(sdf), copycols=false)
    end
end

Base.convert(::Type{DataFrame}, sdf::SubDataFrame) = DataFrame(sdf)

# this function tests if it is allowed to add columns to passed SubDataFrame
# currently it is only allowed when SubDataFrame was created with : as column selector
# which results in using Index as its index (as opposed to other columns selectors
# which result in SubIndex)
function is_column_insertion_allowed(df::AbstractDataFrame)
    if df isa DataFrame
        return true
    elseif df isa SubDataFrame
        return getfield(df, :colindex) isa Index
    end
    throw(ArgumentError("Unsupported data frame type"))
end

function _replace_columns!(sdf::SubDataFrame, newdf::DataFrame)
    colsmatch = _names(sdf) == _names(newdf)

    if !(colsmatch || is_column_insertion_allowed(sdf))
        throw(ArgumentError("changing the sequence of column names in a SubDataFrame " *
                            "that subsets columns of its parent data frame is disallowed"))
    end

    for colname in _names(newdf)
        # This will allocate a fresh column in parent(sdf) for each colname
        sdf[!, colname] = newdf[!, colname]
    end

    # If columns did not match this means that we have either:
    # 1. inserted some columns into pdf
    # or
    # 2. requested to reorder the existing columns
    # and that operation was allowed.
    # Therefore we need to update the parent of sdf in place to make sure
    # it holds only the required target columns in a correct order.
    if !colsmatch
        pdf = parent(sdf)
        @assert pdf isa DataFrame
        select!(pdf, _names(newdf))
    end

    return sdf
end
