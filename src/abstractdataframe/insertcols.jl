const INSERTCOLS_ARGUMENTS =
    """
    If `col` is omitted it is set to `ncol(df)+1`
    (the column is inserted as the last column).

    # Arguments
    - `df` : the data frame to which we want to add columns
    - `col` : a position at which we want to insert a column, passed as an integer
      or a column name (a string or a `Symbol`); the column selected with `col`
      and columns following it are shifted to the right in `df` after the operation
    - `name` : the name of the new column
    - `val` : an `AbstractVector` giving the contents of the new column or a value of any
      type other than `AbstractArray` which will be repeated to fill a new vector;
      As a particular rule a values stored in a `Ref` or a `0`-dimensional `AbstractArray`
      are unwrapped and treated in the same way
    - `after` : if `true` columns are inserted after `col`
    - `makeunique` : defines what to do if `name` already exists in `df`;
      if it is `false` an error will be thrown; if it is `true` a new unique name will
      be generated by adding a suffix
    - `copycols` : whether vectors passed as columns should be copied

    If `val` is an `AbstractRange` then the result of `collect(val)` is inserted.

    If `df` is a `SubDataFrame` then it must have been created with `:` as column selector
    (otherwise an error is thrown). In this case the `copycols` keyword argument
    is ignored (i.e. the added column is always copied) and the parent data frame's
    column is filled with `missing` in rows that are filtered out by `df`.

    If `df` isa `DataFrame` that has no columns and only values
    other than `AbstractVector` are passed then it is used to create a one-element
    column.
    If `df` isa `DataFrame` that has no columns and at least one `AbstractVector` is
    passed then its length is used to determine the number of elements in all
    created columns.
    In all other cases the number of rows in all created columns must match
    `nrow(df)`.
    """

"""
    insertcols(df::AbstractDataFrame[, col], (name=>val)::Pair...;
               after::Bool=false, makeunique::Bool=false, copycols::Bool=true)

Insert a column into a copy of `df` data frame using the [`insertcols!`](@ref)
function and return the newly created data frame.

$INSERTCOLS_ARGUMENTS

$METADATA_FIXED

See also [`insertcols!`](@ref).

# Examples
```jldoctest
julia> df = DataFrame(a=1:3)
3×1 DataFrame
 Row │ a
     │ Int64
─────┼───────
   1 │     1
   2 │     2
   3 │     3

julia> insertcols(df, 1, :b => 'a':'c')
3×2 DataFrame
 Row │ b     a
     │ Char  Int64
─────┼─────────────
   1 │ a         1
   2 │ b         2
   3 │ c         3

julia> insertcols(df, :c => 2:4, :c => 3:5, makeunique=true)
3×3 DataFrame
 Row │ a      c      c_1
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      2      3
   2 │     2      3      4
   3 │     3      4      5

julia> insertcols(df, :a, :d => 7:9, after=true)
3×2 DataFrame
 Row │ a      d
     │ Int64  Int64
─────┼──────────────
   1 │     1      7
   2 │     2      8
   3 │     3      9
```
"""
insertcols(df::AbstractDataFrame, args...;
           after::Bool=false, makeunique::Bool=false, copycols::Bool=true) =
    insertcols!(copy(df), args...;
                after=after, makeunique=makeunique, copycols=copycols)

"""
    insertcols!(df::AbstractDataFrame[, col], (name=>val)::Pair...;
                after::Bool=false, makeunique::Bool=false, copycols::Bool=true)

Insert a column into a data frame in place. Return the updated data frame.

$INSERTCOLS_ARGUMENTS

$METADATA_FIXED
Metadata having other styles is dropped (from parent data frame when `df` is a `SubDataFrame`).

See also [`insertcols`](@ref).

# Examples
```jldoctest
julia> df = DataFrame(a=1:3)
3×1 DataFrame
 Row │ a
     │ Int64
─────┼───────
   1 │     1
   2 │     2
   3 │     3

julia> insertcols!(df, 1, :b => 'a':'c')
3×2 DataFrame
 Row │ b     a
     │ Char  Int64
─────┼─────────────
   1 │ a         1
   2 │ b         2
   3 │ c         3

julia> insertcols!(df, 2, :c => 2:4, :c => 3:5, makeunique=true)
3×4 DataFrame
 Row │ b     c      c_1    a
     │ Char  Int64  Int64  Int64
─────┼───────────────────────────
   1 │ a         2      3      1
   2 │ b         3      4      2
   3 │ c         4      5      3

julia> insertcols!(df, :b, :d => 7:9, after=true)
3×5 DataFrame
 Row │ b     d      c      c_1    a
     │ Char  Int64  Int64  Int64  Int64
─────┼──────────────────────────────────
   1 │ a         7      2      3      1
   2 │ b         8      3      4      2
   3 │ c         9      4      5      3
```
"""
function insertcols!(df::AbstractDataFrame, col::ColumnIndex, name_cols::Pair{Symbol}...;
                     after::Bool=false, makeunique::Bool=false, copycols::Bool=true)
    if !is_column_insertion_allowed(df)
        throw(ArgumentError("insertcols! is only supported for DataFrame, or for " *
                            "SubDataFrame created with `:` as column selector"))
    end
    if !(copycols || df isa DataFrame)
        throw(ArgumentError("copycols=false is only allowed if df isa DataFrame "))
    end
    if col isa SymbolOrString
        col_ind = Int(columnindex(df, col))
        if col_ind == 0
            throw(ArgumentError("column $col does not exist in data frame"))
        end
    else
        col_ind = Int(col)
    end

    if after
        col_ind += 1
    end

    if !(0 < col_ind <= ncol(df) + 1)
        throw(ArgumentError("attempt to insert a column to a data frame with " *
                            "$(ncol(df)) columns at index $col_ind"))
    end

    if !makeunique
        if !allunique(first.(name_cols))
            throw(ArgumentError("Names of columns to be inserted into a data frame " *
                                "must be unique when `makeunique=true`"))
        end
        for (n, _) in name_cols
            if hasproperty(df, n)
                throw(ArgumentError("Column $n is already present in the data frame " *
                                    "which is not allowed when `makeunique=true`"))
            end
        end
    end

    if ncol(df) == 0 && df isa DataFrame
        target_row_count = -1
    else
        target_row_count = nrow(df)
    end

    for (n, v) in name_cols
        if v isa AbstractVector
            if target_row_count == -1
                target_row_count = length(v)
            elseif length(v) != target_row_count
                if target_row_count == nrow(df)
                    throw(DimensionMismatch("length of new column $n which is " *
                                            "$(length(v)) must match the number " *
                                            "of rows in data frame ($(nrow(df)))"))
                else
                    throw(DimensionMismatch("all vectors passed to be inserted into " *
                                            "a data frame must have the same length"))
                end
            end
        elseif v isa AbstractArray && ndims(v) > 1
            throw(ArgumentError("adding AbstractArray other than AbstractVector as " *
                                "a column of a data frame is not allowed"))
        end
    end
    if target_row_count == -1
        target_row_count = 1
    end

    start_col_ind = col_ind
    for (name, item) in name_cols
        if !(item isa AbstractVector)
            if item isa Union{AbstractArray{<:Any, 0}, Ref}
                x = item[]
                item_new = fill!(Tables.allocatecolumn(typeof(x), target_row_count), x)
            else
                @assert !(item isa AbstractArray)
                item_new = fill!(Tables.allocatecolumn(typeof(item), target_row_count), item)
            end
        elseif item isa AbstractRange
            item_new = collect(item)
        elseif copycols && df isa DataFrame
            item_new = copy(item)
        else
            item_new = item
        end

        if df isa DataFrame
            dfp = df
        else
            @assert df isa SubDataFrame
            dfp = parent(df)
            item_new_orig = item_new
            T = eltype(item_new_orig)
            item_new = similar(item_new_orig, Union{T, Missing}, nrow(dfp))
            fill!(item_new, missing)
            item_new[rows(df)] = item_new_orig
        end

        firstindex(item_new) != 1 && _onebased_check_error()

        if ncol(dfp) == 0
            dfp[!, name] = item_new
        else
            if hasproperty(dfp, name)
                @assert makeunique
                k = 1
                while true
                    nn = Symbol("$(name)_$k")
                    if !hasproperty(dfp, nn)
                        name = nn
                        break
                    end
                    k += 1
                end
            end
            insert!(index(dfp), col_ind, name)
            insert!(_columns(dfp), col_ind, item_new)
        end
        col_ind += 1
    end

    delta = col_ind - start_col_ind
    colmetadata_dict = getfield(parent(df), :colmetadata)
    if !isnothing(colmetadata_dict) && delta > 0
        to_move = Int[i for i in keys(colmetadata_dict) if i >= start_col_ind]
        sort!(to_move, rev=true)
        for i in to_move
            colmetadata_dict[i + delta] = pop!(colmetadata_dict, i)
        end
    end
    _drop_all_nonnote_metadata!(parent(df))
    return df
end

insertcols!(df::AbstractDataFrame, col::ColumnIndex, name_cols::Pair{<:AbstractString}...;
            after::Bool=false, makeunique::Bool=false, copycols::Bool=true) =
    insertcols!(df, col, (Symbol(n) => v for (n, v) in name_cols)...,
                after=after, makeunique=makeunique, copycols=copycols)

insertcols!(df::AbstractDataFrame, name_cols::Pair{Symbol}...;
            after::Bool=false, makeunique::Bool=false, copycols::Bool=true) =
    insertcols!(df, ncol(df)+1, name_cols..., after=after,
                makeunique=makeunique, copycols=copycols)

insertcols!(df::AbstractDataFrame, name_cols::Pair{<:AbstractString}...;
            after::Bool=false, makeunique::Bool=false, copycols::Bool=true) =
    insertcols!(df, (Symbol(n) => v for (n, v) in name_cols)...,
                after=after, makeunique=makeunique, copycols=copycols)

function insertcols!(df::AbstractDataFrame, col::ColumnIndex; after::Bool=false,
                     makeunique::Bool=false, copycols::Bool=true)
    if col isa SymbolOrString
        col_ind = Int(columnindex(df, col))
        if col_ind == 0
            throw(ArgumentError("column $col does not exist in data frame"))
        end
    else
        col_ind = Int(col)
    end

    if after
        col_ind += 1
    end

    if !(0 < col_ind <= ncol(df) + 1)
        throw(ArgumentError("attempt to insert a column to a data frame with " *
                            "$(ncol(df)) columns at index $col_ind"))
    end

    _drop_all_nonnote_metadata!(parent(df))
    return df
end

function insertcols!(df::AbstractDataFrame; after::Bool=false,
                     makeunique::Bool=false, copycols::Bool=true)
    _drop_all_nonnote_metadata!(parent(df))
    return df
end

