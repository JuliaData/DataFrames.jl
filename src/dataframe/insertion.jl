"""
    append!(df::DataFrame, tables...; cols::Symbol=:setequal,
            promote::Bool=(cols in [:union, :subset]))

Add the rows of tables passed as `tables` to the end of `df`. If the table is not
an `AbstractDataFrame` then it is converted using
`DataFrame(table, copycols=false)` before being appended.

The exact behavior of `append!` depends on the `cols` argument:
* If `cols == :setequal` (this is the default) then `df2` must contain exactly
  the same columns as `df` (but possibly in a different order).
* If `cols == :orderequal` then `df2` must contain the same columns in the same
  order (for `AbstractDict` this option requires that `keys(row)` matches
  `propertynames(df)` to allow for support of ordered dicts; however, if `df2`
  is a `Dict` an error is thrown as it is an unordered collection).
* If `cols == :intersect` then `df2` may contain more columns than `df`, but all
  column names that are present in `df` must be present in `df2` and only these
  are used.
* If `cols == :subset` then `append!` behaves like for `:intersect` but if some
  column is missing in `df2` then a `missing` value is pushed to `df`.
* If `cols == :union` then `append!` adds columns missing in `df` that are
  present in `df2`, for columns present in `df` but missing in `df2` a `missing`
  value is pushed.

If `promote=true` and element type of a column present in `df` does not allow
the type of a pushed argument then a new column with a promoted element type
allowing it is freshly allocated and stored in `df`. If `promote=false` an error
is thrown.

The above rule has the following exceptions:
* If `df` has no columns then copies of columns from `df2` are added to it.
* If `df2` has no columns then calling `append!` leaves `df` unchanged.

Please note that `append!` must not be used on a `DataFrame` that contains
columns that are aliases (equal when compared with `===`).

Metadata: table-level `:note`-style metadata and column-level `:note`-style metadata for
columns present in `df` are preserved. If new columns are added their
`:note`-style metadata is copied from the appended table. Other metadata is
dropped.

See also: use [`push!`](@ref) to add individual rows to a data frame,
[`prepend!`](@ref) to add a table at the beginning, and [`vcat`](@ref) to
vertically concatenate data frames.

# Examples
```jldoctest
julia> df1 = DataFrame(A=1:3, B=1:3)
3×2 DataFrame
 Row │ A      B
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      3

julia> df2 = DataFrame(A=4.0:6.0, B=4:6)
3×2 DataFrame
 Row │ A        B
     │ Float64  Int64
─────┼────────────────
   1 │     4.0      4
   2 │     5.0      5
   3 │     6.0      6

julia> append!(df1, df2);

julia> df1
6×2 DataFrame
 Row │ A      B
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      3
   4 │     4      4
   5 │     5      5
   6 │     6      6

julia> append!(df2, DataFrame(A=1), (; C=1:2), cols=:union)
6×3 DataFrame
 Row │ A          B        C
     │ Float64?   Int64?   Int64?
─────┼─────────────────────────────
   1 │       4.0        4  missing
   2 │       5.0        5  missing
   3 │       6.0        6  missing
   4 │       1.0  missing  missing
   5 │ missing    missing        1
   6 │ missing    missing        2
```
"""
Base.append!(df1::DataFrame, df2::AbstractDataFrame; cols::Symbol=:setequal,
             promote::Bool=(cols in [:union, :subset])) =
    _append_or_prepend!(df1, df2, cols=cols, promote=promote, atend=true)

function Base.append!(df::DataFrame, table; cols::Symbol=:setequal,
                      promote::Bool=(cols in [:union, :subset]))
    if table isa Dict && cols == :orderequal
        throw(ArgumentError("passing `Dict` as `table` when `cols` is equal to " *
                            "`:orderequal` is not allowed as it is unordered"))
    end
    append!(df, DataFrame(table, copycols=false), cols=cols, promote=promote)
end

function Base.append!(df::DataFrame, @nospecialize tables...;
                      cols::Symbol=:setequal,
                      promote::Bool=(cols in [:union, :subset]))
    if !(cols in (:orderequal, :setequal, :intersect, :subset, :union))
        throw(ArgumentError("`cols` keyword argument must be " *
                            ":orderequal, :setequal, :intersect, :subset or :union)"))
    end

    return foldl((df, table) -> append!(df, table, cols=cols, promote=promote),
                 collect(Any, tables), init=df)
end

"""
    prepend!(df::DataFrame, tables...; cols::Symbol=:setequal,
             promote::Bool=(cols in [:union, :subset]))

Add the rows of tables passed as `tables` to the beginning of `df`. If the table is not
an `AbstractDataFrame` then it is converted using
`DataFrame(table, copycols=false)` before being appended.

Add the rows of `df2` to the beginning of `df`. If the second argument `table`
is not an `AbstractDataFrame` then it is converted using `DataFrame(table,
copycols=false)` before being prepended.

The exact behavior of `prepend!` depends on the `cols` argument:
* If `cols == :setequal` (this is the default) then `df2` must contain exactly
  the same columns as `df` (but possibly in a different order).
* If `cols == :orderequal` then `df2` must contain the same columns in the same
  order (for `AbstractDict` this option requires that `keys(row)` matches
  `propertynames(df)` to allow for support of ordered dicts; however, if `df2`
  is a `Dict` an error is thrown as it is an unordered collection).
* If `cols == :intersect` then `df2` may contain more columns than `df`, but all
  column names that are present in `df` must be present in `df2` and only these
  are used.
* If `cols == :subset` then `append!` behaves like for `:intersect` but if some
  column is missing in `df2` then a `missing` value is pushed to `df`.
* If `cols == :union` then `append!` adds columns missing in `df` that are
  present in `df2`, for columns present in `df` but missing in `df2` a `missing`
  value is pushed.

If `promote=true` and element type of a column present in `df` does not allow
the type of a pushed argument then a new column with a promoted element type
allowing it is freshly allocated and stored in `df`. If `promote=false` an error
is thrown.

The above rule has the following exceptions:
* If `df` has no columns then copies of columns from `df2` are added to it.
* If `df2` has no columns then calling `prepend!` leaves `df` unchanged.

Please note that `prepend!` must not be used on a `DataFrame` that contains
columns that are aliases (equal when compared with `===`).

Metadata: table-level `:note`-style metadata and column-level `:note`-style metadata for
columns present in `df` are preserved. If new columns are added their
`:note`-style metadata is copied from the appended table. Other metadata is
dropped.

See also: use [`pushfirst!`](@ref) to add individual rows at the beginning of a
data frame, [`append!`](@ref) to add a table at the end, and [`vcat`](@ref) to
vertically concatenate data frames.

# Examples
```jldoctest
julia> df1 = DataFrame(A=1:3, B=1:3)
3×2 DataFrame
 Row │ A      B
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      3

julia> df2 = DataFrame(A=4.0:6.0, B=4:6)
3×2 DataFrame
 Row │ A        B
     │ Float64  Int64
─────┼────────────────
   1 │     4.0      4
   2 │     5.0      5
   3 │     6.0      6

julia> prepend!(df1, df2);

julia> df1
6×2 DataFrame
 Row │ A      B
     │ Int64  Int64
─────┼──────────────
   1 │     4      4
   2 │     5      5
   3 │     6      6
   4 │     1      1
   5 │     2      2
   6 │     3      3

julia> prepend!(df2, DataFrame(A=1), (; C=1:2), cols=:union)
6×3 DataFrame
 Row │ A          B        C
     │ Float64?   Int64?   Int64?
─────┼─────────────────────────────
   1 │       1.0  missing  missing
   2 │ missing    missing        1
   3 │ missing    missing        2
   4 │       4.0        4  missing
   5 │       5.0        5  missing
   6 │       6.0        6  missing
```
"""
Base.prepend!(df1::DataFrame, df2::AbstractDataFrame; cols::Symbol=:setequal,
              promote::Bool=(cols in [:union, :subset])) =
    _append_or_prepend!(df1, df2, cols=cols, promote=promote, atend=false)

function Base.prepend!(df::DataFrame, table; cols::Symbol=:setequal,
                       promote::Bool=(cols in [:union, :subset]))
    if table isa Dict && cols == :orderequal
        throw(ArgumentError("passing `Dict` as `table` when `cols` is equal to " *
                            "`:orderequal` is not allowed as it is unordered"))
    end
    prepend!(df, DataFrame(table, copycols=false), cols=cols, promote=promote)
end

function Base.prepend!(df::DataFrame, @nospecialize tables...;
                       cols::Symbol=:setequal,
                       promote::Bool=(cols in [:union, :subset]))
    if !(cols in (:orderequal, :setequal, :intersect, :subset, :union))
        throw(ArgumentError("`cols` keyword argument must be " *
                            ":orderequal, :setequal, :intersect, :subset or :union)"))
    end

    return foldr((table, df) -> prepend!(df, table, cols=cols, promote=promote),
                 collect(Any, tables), init=df)
end

function _append_or_prepend!(df1::DataFrame, df2::AbstractDataFrame; cols::Symbol,
                             promote::Bool, atend::Bool)
    if !(cols in (:orderequal, :setequal, :intersect, :subset, :union))
        throw(ArgumentError("`cols` keyword argument must be " *
                            ":orderequal, :setequal, :intersect, :subset or :union)"))
    end

    _drop_all_nonnote_metadata!(df1)
    if ncol(df1) == 0
        for (n, v) in pairs(eachcol(df2))
            df1[!, n] = copy(v) # make sure df1 does not reuse df2
            _copy_col_note_metadata!(df1, n, df2, n)
        end
        return df1
    end
    ncol(df2) == 0 && return df1

    if cols == :orderequal && _names(df1) != _names(df2)
        wrongnames = symdiff(_names(df1), _names(df2))
        if isempty(wrongnames)
            mismatches = findall(_names(df1) .!= _names(df2))
            @assert !isempty(mismatches)
            throw(ArgumentError("Columns number " *
                                join(mismatches, ", ", " and ") *
                                " do not have the same names in both passed " *
                                "data frames and `cols == :orderequal`"))
        else
            mismatchmsg = " Column names :" *
            throw(ArgumentError("Column names :" *
                                join(wrongnames, ", :", " and :") *
                                " were found in only one of the passed data frames " *
                                "and `cols == :orderequal`"))
        end
    elseif cols == :setequal
        wrongnames = symdiff(_names(df1), _names(df2))
        if !isempty(wrongnames)
            throw(ArgumentError("Column names :" *
                                join(wrongnames, ", :", " and :") *
                                " were found in only one of the passed data frames " *
                                "and `cols == :setequal`"))
        end
    elseif cols == :intersect
        wrongnames = setdiff(_names(df1), _names(df2))
        if !isempty(wrongnames)
            throw(ArgumentError("Column names :" *
                                join(wrongnames, ", :", " and :") *
                                " were found in only in destination data frame " *
                                "and `cols == :intersect`"))
        end
    end

    nrow1 = nrow(df1)
    nrow2 = nrow(df2)
    targetrows = nrow1 + nrow2
    current_col = 0
    # in the code below we use a direct access to _columns because
    # we resize the columns so temporarily the `DataFrame` is internally
    # inconsistent and normal data frame indexing would error.
    try
        for (j, n) in enumerate(_names(df1))
            current_col += 1
            if hasproperty(df2, n)
                df2_c = df2[!, n]
                S = eltype(df2_c)
                df1_c = df1[!, j]
                T = eltype(df1_c)
                if S <: T || !promote || promote_type(S, T) <: T
                    # if S <: T || promote_type(S, T) <: T this should never throw an exception
                    if atend
                        append!(df1_c, df2_c)
                    else
                        prepend!(df1_c, df2_c)
                    end
                else
                    newcol = similar(df1_c, promote_type(S, T), targetrows)
                    firstindex(newcol) != 1 && _onebased_check_error()
                    if atend
                        copyto!(newcol, 1, df1_c, 1, nrow1)
                        copyto!(newcol, nrow1+1, df2_c, 1, nrow2)
                    else
                        copyto!(newcol, 1, df2_c, 1, nrow2)
                        copyto!(newcol, nrow2+1, df1_c, 1, nrow1)
                    end
                    _columns(df1)[j] = newcol
                end
            else
                if Missing <: eltype(df1[!, j])
                    if atend
                        resize!(df1[!, j], targetrows)
                        df1[nrow1+1:targetrows, j] .= missing
                    else
                        prepend!(df1[!, j], Iterators.repeated(missing, nrow2))
                    end
                elseif promote
                    newcol = similar(df1[!, j], Union{Missing, eltype(df1[!, j])},
                                     targetrows)
                    firstindex(newcol) != 1 && _onebased_check_error()
                    if atend
                        copyto!(newcol, 1, df1[!, j], 1, nrow1)
                        newcol[nrow1+1:targetrows] .= missing
                    else
                        copyto!(newcol, nrow2+1, df1[!, j], 1, nrow1)
                        newcol[1:nrow2] .= missing
                    end
                    _columns(df1)[j] = newcol
                else
                    throw(ArgumentError("promote=false and source data frame does " *
                                        "not contain column :$n, while destination " *
                                        "column does not allow for missing values"))
                end
            end
        end
        current_col = 0
        for col in _columns(df1)
            current_col += 1
            @assert length(col) == targetrows
        end
    catch err
        # Undo changes in case of error
        for col in _columns(df1)
            @assert length(col) >= nrow1
            if atend
                resize!(col, nrow1)
            elseif length(col) != nrow1
                deleteat!(col, 1:length(col) - nrow1)
            end
        end
        if promote
            @error "Error adding value to column :$(_names(df1)[current_col])."
        else
            @error "Error adding value to column :$(_names(df1)[current_col]). " *
                   "Maybe you forgot passing `promote=true`?"
        end
        rethrow(err)
    end

    if cols == :union
        for n in setdiff(_names(df2), _names(df1))
            newcol = similar(df2[!, n], Union{Missing, eltype(df2[!, n])},
                             targetrows)
            firstindex(newcol) != 1 && _onebased_check_error()
            if atend
                newcol[1:nrow1] .= missing
                copyto!(newcol, nrow1+1, df2[!, n], 1, targetrows - nrow1)
            else
                newcol[nrow2+1:targetrows] .= missing
                copyto!(newcol, 1, df2[!, n], 1, nrow2)
            end
            df1[!, n] = newcol
            _copy_col_note_metadata!(df1, n, df2, n)
        end
    end

    return df1
end

const INSERTION_COMMON = """
Column types of `df` are preserved, and new values are converted if necessary.
An error is thrown if conversion fails.

If `row` is neither a `DataFrameRow`, `NamedTuple` nor `AbstractDict` then
it must be a `Tuple` or an `AbstractArray`
and columns are matched by order of appearance. In this case `row` must contain
the same number of elements as the number of columns in `df`.

If `row` is a `DataFrameRow`, `NamedTuple`, `AbstractDict`, or
`Tables.AbstractRow` then values in `row` are matched to columns in `df` based
on names. The exact behavior depends on the `cols` argument value in the
following way:
* If `cols == :setequal` (this is the default)
  then `row` must contain exactly the same columns as `df` (but possibly in a
  different order).
* If `cols == :orderequal` then `row` must contain the same columns in the same
  order (for `AbstractDict` this option requires that `keys(row)` matches
  `propertynames(df)` to allow for support of ordered dicts; however, if `row`
  is a `Dict` an error is thrown as it is an unordered collection).
* If `cols == :intersect` then `row` may contain more columns than `df`,
  but all column names that are present in `df` must be present in `row` and only
  they are used to populate a new row in `df`.
* If `cols == :subset` then the behavior is like for `:intersect` but if some
  column is missing in `row` then a `missing` value is pushed to `df`.
* If `cols == :union` then columns missing in `df` that are present in `row` are
  added to `df` (using `missing` for existing rows) and a `missing` value is
  pushed to columns missing in `row` that are present in `df`.

If `row` is not a `DataFrameRow`, `NamedTuple`, `AbstractDict`, or `Tables.AbstractRow`
the `cols` keyword argument must be `:setequal` (the default),
because such rows do not provide column name information.

If `promote=true` and element type of a column present in `df` does not allow
the type of a pushed argument then a new column with a promoted element type
allowing it is freshly allocated and stored in `df`. If `promote=false` an error
is thrown.

As a special case, if `df` has no columns and `row` is a `NamedTuple`,
`DataFrameRow`, or `Tables.AbstractRow`, columns are created for all values in
`row`, using their names and order.

Please note that this function must not be used on a
`DataFrame` that contains columns that are aliases (equal when compared with `===`).

$METADATA_FIXED
"""

"""
    push!(df::DataFrame, row::Union{Tuple, AbstractArray}...;
          cols::Symbol=:setequal, promote::Bool=false)
    push!(df::DataFrame, row::Union{DataFrameRow, NamedTuple, AbstractDict,
                                    Tables.AbstractRow}...;
          cols::Symbol=:setequal, promote::Bool=(cols in [:union, :subset]))

Add one row at the end of `df` in-place, taking the values from `row`.
Several rows can be added by passing them as separate arguments.

$INSERTION_COMMON

See also: [`pushfirst!`](@ref), [`insert!`](@ref)

# Examples
```jldoctest
julia> df = DataFrame(A='a':'c', B=1:3)
3×2 DataFrame
 Row │ A     B
     │ Char  Int64
─────┼─────────────
   1 │ a         1
   2 │ b         2
   3 │ c         3

julia> push!(df, (true, false), promote=true)
4×2 DataFrame
 Row │ A     B
     │ Any   Int64
─────┼─────────────
   1 │ a         1
   2 │ b         2
   3 │ c         3
   4 │ true      0

julia> push!(df, df[1, :])
5×2 DataFrame
 Row │ A     B
     │ Any   Int64
─────┼─────────────
   1 │ a         1
   2 │ b         2
   3 │ c         3
   4 │ true      0
   5 │ a         1

julia> push!(df, (C="something", A=11, B=12), cols=:intersect)
6×2 DataFrame
 Row │ A     B
     │ Any   Int64
─────┼─────────────
   1 │ a         1
   2 │ b         2
   3 │ c         3
   4 │ true      0
   5 │ a         1
   6 │ 11       12

julia> push!(df, Dict(:A=>1.0, :C=>1.0), cols=:union)
7×3 DataFrame
 Row │ A     B        C
     │ Any   Int64?   Float64?
─────┼──────────────────────────
   1 │ a           1  missing
   2 │ b           2  missing
   3 │ c           3  missing
   4 │ true        0  missing
   5 │ a           1  missing
   6 │ 11         12  missing
   7 │ 1.0   missing        1.0

julia> push!(df, NamedTuple(), cols=:subset)
8×3 DataFrame
 Row │ A        B        C
     │ Any      Int64?   Float64?
─────┼─────────────────────────────
   1 │ a              1  missing
   2 │ b              2  missing
   3 │ c              3  missing
   4 │ true           0  missing
   5 │ a              1  missing
   6 │ 11            12  missing
   7 │ 1.0      missing        1.0
   8 │ missing  missing  missing

julia> push!(DataFrame(a=1, b=2), (3, 4), (5, 6))
3×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      2
   2 │     3      4
   3 │     5      6
```
"""
function Base.push!(df::DataFrame, row::Any;
                    cols=:setequal, promote::Bool=false)
    if cols !== :setequal
        throw(ArgumentError("`cols` can only be `:setequal` when `row` is a `$(typeof(row))` " *
                            "as this type does not provide column names"))
    end

    return _row_inserter!(df, -1, row, Val{:push}(), promote)
end

"""
    pushfirst!(df::DataFrame, row::Union{Tuple, AbstractArray}...;
               cols::Symbol=:setequal, promote::Bool=false)
    pushfirst!(df::DataFrame, row::Union{DataFrameRow, NamedTuple, AbstractDict,
                                         Tables.AbstractRow}...;
               cols::Symbol=:setequal, promote::Bool=(cols in [:union, :subset]))

Add one row at the beginning of `df` in-place, taking the values from `row`.
Several rows can be added by passing them as separate arguments.

$INSERTION_COMMON

See also: [`push!`](@ref), [`insert!`](@ref)

# Examples
```jldoctest
julia> df = DataFrame(A='a':'c', B=1:3)
3×2 DataFrame
 Row │ A     B
     │ Char  Int64
─────┼─────────────
   1 │ a         1
   2 │ b         2
   3 │ c         3

julia> pushfirst!(df, (true, false), promote=true)
4×2 DataFrame
 Row │ A     B
     │ Any   Int64
─────┼─────────────
   1 │ true      0
   2 │ a         1
   3 │ b         2
   4 │ c         3

julia> pushfirst!(df, df[1, :])
5×2 DataFrame
 Row │ A     B
     │ Any   Int64
─────┼─────────────
   1 │ true      0
   2 │ true      0
   3 │ a         1
   4 │ b         2
   5 │ c         3

julia> pushfirst!(df, (C="something", A=11, B=12), cols=:intersect)
6×2 DataFrame
 Row │ A     B
     │ Any   Int64
─────┼─────────────
   1 │ 11       12
   2 │ true      0
   3 │ true      0
   4 │ a         1
   5 │ b         2
   6 │ c         3

julia> pushfirst!(df, Dict(:A=>1.0, :C=>1.0), cols=:union)
7×3 DataFrame
 Row │ A     B        C
     │ Any   Int64?   Float64?
─────┼──────────────────────────
   1 │ 1.0   missing        1.0
   2 │ 11         12  missing
   3 │ true        0  missing
   4 │ true        0  missing
   5 │ a           1  missing
   6 │ b           2  missing
   7 │ c           3  missing

julia> pushfirst!(df, NamedTuple(), cols=:subset)
8×3 DataFrame
 Row │ A        B        C
     │ Any      Int64?   Float64?
─────┼─────────────────────────────
   1 │ missing  missing  missing
   2 │ 1.0      missing        1.0
   3 │ 11            12  missing
   4 │ true           0  missing
   5 │ true           0  missing
   6 │ a              1  missing
   7 │ b              2  missing
   8 │ c              3  missing

julia> pushfirst!(DataFrame(a=1, b=2), (3, 4), (5, 6))
3×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     3      4
   2 │     5      6
   3 │     1      2
```
"""
function Base.pushfirst!(df::DataFrame, row::Any;
                         cols=:setequal, promote::Bool=false)
    if cols !== :setequal
        throw(ArgumentError("`cols` can only be `:setequal` when `row` is a `$(typeof(row))` " *
                            "as this type does not provide column names"))
    end

    return _row_inserter!(df, -1, row, Val{:pushfirst}(), promote)
end

"""
    insert!(df::DataFrame, index::Integer, row::Union{Tuple, AbstractArray};
            cols::Symbol=:setequal, promote::Bool=false)
    insert!(df::DataFrame, index::Integer, row::Union{DataFrameRow, NamedTuple,
                                                      AbstractDict, Tables.AbstractRow};
            cols::Symbol=:setequal, promote::Bool=(cols in [:union, :subset]))

Add one row to `df` at position `index` in-place, taking the values from `row`.
`index` must be a integer between `1` and `nrow(df)+1`.

$INSERTION_COMMON

See also: [`push!`](@ref), [`pushfirst!`](@ref)

# Examples
```jldoctest
julia> df = DataFrame(A='a':'c', B=1:3)
3×2 DataFrame
 Row │ A     B
     │ Char  Int64
─────┼─────────────
   1 │ a         1
   2 │ b         2
   3 │ c         3

julia> insert!(df, 2, (true, false), promote=true)
4×2 DataFrame
 Row │ A     B
     │ Any   Int64
─────┼─────────────
   1 │ a         1
   2 │ true      0
   3 │ b         2
   4 │ c         3

julia> insert!(df, 5, df[1, :])
5×2 DataFrame
 Row │ A     B
     │ Any   Int64
─────┼─────────────
   1 │ a         1
   2 │ true      0
   3 │ b         2
   4 │ c         3
   5 │ a         1

julia> insert!(df, 1, (C="something", A=11, B=12), cols=:intersect)
6×2 DataFrame
 Row │ A     B
     │ Any   Int64
─────┼─────────────
   1 │ 11       12
   2 │ a         1
   3 │ true      0
   4 │ b         2
   5 │ c         3
   6 │ a         1

julia> insert!(df, 7, Dict(:A=>1.0, :C=>1.0), cols=:union)
7×3 DataFrame
 Row │ A     B        C
     │ Any   Int64?   Float64?
─────┼──────────────────────────
   1 │ 11         12  missing
   2 │ a           1  missing
   3 │ true        0  missing
   4 │ b           2  missing
   5 │ c           3  missing
   6 │ a           1  missing
   7 │ 1.0   missing        1.0

julia> insert!(df, 3, NamedTuple(), cols=:subset)
8×3 DataFrame
 Row │ A        B        C
     │ Any      Int64?   Float64?
─────┼─────────────────────────────
   1 │ 11            12  missing
   2 │ a              1  missing
   3 │ missing  missing  missing
   4 │ true           0  missing
   5 │ b              2  missing
   6 │ c              3  missing
   7 │ a              1  missing
   8 │ 1.0      missing        1.0
```
"""
function Base.insert!(df::DataFrame, index::Integer, row::Any;
                      cols=:setequal, promote::Bool=false)
    if cols !== :setequal
        throw(ArgumentError("`cols` can only be `:setequal` when `row` is a `$(typeof(row))` " *
                            "as this type does not provide column names"))
    end

    index isa Bool && throw(ArgumentError("invalid index: $index of type Bool"))
    1 <= index <= nrow(df)+1 ||
        throw(ArgumentError("invalid index: $index for data frame with $(nrow(df)) rows"))
    return _row_inserter!(df, index, row, Val{:insert}(), promote)
end

function _row_inserter!(df::DataFrame, loc::Integer, row::Any,
                        mode::Union{Val{:push}, Val{:pushfirst}, Val{:insert}},
                        promote::Bool)
    if !(row isa Union{Tuple, AbstractArray})
        # an explicit error is thrown as this was allowed in the past
        throw(ArgumentError("it is not allowed to insert collections of type " *
                            "$(typeof(row)) into a DataFrame. Only " *
                            "`Tuple`, `AbstractArray`, `AbstractDict`, `DataFrameRow` " *
                            "and `NamedTuple` are allowed."))
    end
    nrows, ncols = size(df)
    targetrows = nrows + 1
    if length(row) != ncols
        msg = "Length of `row` does not match `DataFrame` column count."
        throw(DimensionMismatch(msg))
    end
    current_col = 0
    try
        for (i, (col, val)) in enumerate(zip(_columns(df), row))
            current_col += 1
            @assert length(col) == nrows
            S = typeof(val)
            T = eltype(col)
            if S <: T || !promote || promote_type(S, T) <: T
                mode isa Val{:push} && push!(col, val)
                mode isa Val{:pushfirst} && pushfirst!(col, val)
                mode isa Val{:insert} && insert!(col, loc, val)
            else
                newcol = Tables.allocatecolumn(promote_type(S, T), targetrows)
                firstindex(newcol) != 1 && _onebased_check_error()
                if mode isa Val{:push}
                    copyto!(newcol, 1, col, 1, nrows)
                    newcol[end] = val
                elseif mode isa Val{:pushfirst}
                    newcol[1] = val
                    copyto!(newcol, 2, col, 1, nrows)
                elseif mode isa Val{:insert}
                    copyto!(newcol, 1, col, 1, loc-1)
                    newcol[loc] = val
                    copyto!(newcol, loc+1, col, loc, nrows-loc+1)
                end
                _columns(df)[i] = newcol
            end
        end
    catch err
        # clean up partial row
        for j in 1:current_col
            col2 = _columns(df)[j]
            if length(col2) == targetrows
                mode isa Val{:push} && pop!(col2)
                mode isa Val{:pushfirst} && popfirst!(col2)
                mode isa Val{:insert} && deleteat!(col2, loc)
            end
            @assert length(col2) == nrows
        end
        if promote
            @error "Error adding value to column :$(_names(df)[current_col])."
        else
            @error "Error adding value to column :$(_names(df)[current_col]). " *
                   "Maybe you forgot passing `promote=true`?"
        end
        rethrow(err)
    end
    _drop_all_nonnote_metadata!(df)
    return df
end

Base.push!(df::DataFrame, row::DataFrameRow;
           cols::Symbol=:setequal,
           promote::Bool=(cols in [:union, :subset])) =
    _dfr_row_inserter!(df, -1, row, Val{:push}(), cols, promote)

Base.pushfirst!(df::DataFrame, row::DataFrameRow;
                cols::Symbol=:setequal,
                promote::Bool=(cols in [:union, :subset])) =
    _dfr_row_inserter!(df, -1, row, Val{:pushfirst}(), cols, promote)

function Base.insert!(df::DataFrame, index::Integer, row::DataFrameRow;
                      cols::Symbol=:setequal, promote::Bool=(cols in [:union, :subset]))
    index isa Bool && throw(ArgumentError("invalid index: $index of type Bool"))
    1 <= index <= nrow(df)+1 ||
        throw(ArgumentError("invalid index: $index for data frame with $(nrow(df)) rows"))
    _dfr_row_inserter!(df, index, row, Val{:insert}(), cols, promote)
end

@noinline pushhelper!(x::AbstractVector, r::Any) =
    push!(x, x[r])

@noinline pushfirsthelper!(x::AbstractVector, r::Any) =
    pushfirst!(x, x[r])

@noinline inserthelper!(x::AbstractVector, loc::Integer, r::Any) =
    insert!(x, loc, x[r])

function _dfr_row_inserter!(df::DataFrame, loc::Integer, dfr::DataFrameRow,
                            mode::Union{Val{:push}, Val{:pushfirst}, Val{:insert}},
                            cols::Symbol, promote::Bool)
    possible_cols = (:orderequal, :setequal, :intersect, :subset, :union)
    if !(cols in possible_cols)
        throw(ArgumentError("`cols` keyword argument must be any of :" *
                            join(possible_cols, ", :")))
    end

    nrows = nrow(df)
    targetrows = nrows + 1

    if parent(dfr) === df && index(dfr) isa Index
        # in this case we are sure that all we do is safe
        r = row(dfr)
        for (col_num, col) in enumerate(_columns(df))
            if length(col) != nrows
                for j in 1:col_num
                    col2 = _columns(df)[j]
                    if length(col2) == targetrows
                        mode isa Val{:push} && pop!(col2)
                        mode isa Val{:pushfirst} && popfirst!(col2)
                        mode isa Val{:insert} && deleteat!(col2, loc)
                    end
                    @assert length(col2) == nrows
                end
                colname = _names(df)[col_num]
                throw(AssertionError("Error adding value to column :$colname."))
            end
            # use a function barrier to improve performance
            mode isa Val{:push} && pushhelper!(col, r)
            mode isa Val{:pushfirst} && pushfirsthelper!(col, r)
            mode isa Val{:insert} && inserthelper!(col, loc, r)
        end
        _drop_all_nonnote_metadata!(df)
        return df
    end

    return _row_inserter!(df, loc, dfr, mode, cols, promote, nrows)
end

Base.push!(df::DataFrame,
           row::Union{AbstractDict, NamedTuple, Tables.AbstractRow};
           cols::Symbol=:setequal,
           promote::Bool=(cols in [:union, :subset])) =
    _row_inserter!(df, -1, row, Val{:push}(), cols, promote, -1)

Base.pushfirst!(df::DataFrame,
                row::Union{AbstractDict, NamedTuple, Tables.AbstractRow};
                cols::Symbol=:setequal,
                promote::Bool=(cols in [:union, :subset])) =
    _row_inserter!(df, -1, row, Val{:pushfirst}(), cols, promote, -1)

function Base.insert!(df::DataFrame, loc::Integer,
                      row::Union{AbstractDict, NamedTuple, Tables.AbstractRow};
                      cols::Symbol=:setequal,
                      promote::Bool=(cols in [:union, :subset]))
    loc isa Bool && throw(ArgumentError("invalid index: $loc of type Bool"))
    1 <= loc <= nrow(df)+1 ||
        throw(ArgumentError("invalid index: $loc for data frame with $(nrow(df)) rows"))
    return _row_inserter!(df, loc, row, Val{:insert}(), cols, promote, -1)
end

function _row_inserter!(df::DataFrame, loc::Integer,
                        row::Union{AbstractDict, NamedTuple, DataFrameRow,
                                   Tables.AbstractRow},
                        mode::Union{Val{:push}, Val{:pushfirst}, Val{:insert}},
                        cols::Symbol, promote::Bool, nrows::Int)
    if nrows == -1
        @assert row isa Union{AbstractDict, NamedTuple, Tables.AbstractRow}
        possible_cols = (:orderequal, :setequal, :intersect, :subset, :union)
        if !(cols in possible_cols)
            throw(ArgumentError("`cols` keyword argument must be any of :" *
                                join(possible_cols, ", :")))
        end
        nrows = nrow(df)
    else
        @assert row isa DataFrameRow
    end

    ncols = ncol(df)
    targetrows = nrows + 1

    if ncols == 0 && row isa Union{NamedTuple, DataFrameRow, Tables.AbstractRow}
        for (n, v) in pairs(row)
            setproperty!(df, n, fill!(Tables.allocatecolumn(typeof(v), 1), v))
        end
        _drop_all_nonnote_metadata!(df)
        return df
    end

    old_row_type = typeof(row)
    if row isa AbstractDict && keytype(row) !== Symbol &&
        (keytype(row) <: AbstractString || all(x -> x isa AbstractString, keys(row)))
        row = (;(Symbol.(keys(row)) .=> values(row))...)
    end

    # in the code below we use a direct access to _columns because
    # we resize the columns so temporarily the `DataFrame` is internally
    # inconsistent and normal data frame indexing would error.
    if cols == :union
        if row isa AbstractDict && keytype(row) !== Symbol && !all(x -> x isa Symbol, keys(row))
            throw(ArgumentError("when `cols == :union` all keys of row must be Symbol"))
        end
        for (i, colname) in enumerate(_names(df))
            col = _columns(df)[i]
            if length(col) != nrows
                for j in 1:i
                    col2 = _columns(df)[j]
                    if length(col2) == targetrows
                        mode isa Val{:push} && pop!(col2)
                        mode isa Val{:pushfirst} && popfirst!(col2)
                        mode isa Val{:insert} && deleteat!(col2, loc)
                    end
                    @assert length(col2) == nrows
                end
                throw(AssertionError("Error adding value to column :$colname."))
            end
            val = get(row, colname, missing)
            S = typeof(val)
            T = eltype(col)
            if S <: T || promote_type(S, T) <: T
                mode isa Val{:push} && push!(col, val)
                mode isa Val{:pushfirst} && pushfirst!(col, val)
                mode isa Val{:insert} && insert!(col, loc, val)
            elseif !promote
                try
                    mode isa Val{:push} && push!(col, val)
                    mode isa Val{:pushfirst} && pushfirst!(col, val)
                    mode isa Val{:insert} && insert!(col, loc, val)
                catch err
                    for j in 1:i
                        col2 = _columns(df)[j]
                        if length(col2) == targetrows
                            mode isa Val{:push} && pop!(col2)
                            mode isa Val{:pushfirst} && popfirst!(col2)
                            mode isa Val{:insert} && deleteat!(col2, loc)
                        end
                        @assert length(col2) == nrows
                    end
                    @error "Error adding value to column :$colname. " *
                           "Maybe you forgot passing `promote=true`?"
                    rethrow(err)
                end
            else
                newcol = similar(col, promote_type(S, T), targetrows)
                firstindex(newcol) != 1 && _onebased_check_error()
                if mode isa Val{:push}
                    copyto!(newcol, 1, col, 1, nrows)
                    newcol[end] = val
                elseif mode isa Val{:pushfirst}
                    newcol[1] = val
                    copyto!(newcol, 2, col, 1, nrows)
                elseif mode isa Val{:insert}
                    copyto!(newcol, 1, col, 1, loc-1)
                    newcol[loc] = val
                    copyto!(newcol, loc+1, col, loc, nrows-loc+1)
                end
                _columns(df)[i] = newcol
            end
        end
        for colname in setdiff(keys(row), _names(df))
            val = row[colname]
            S = typeof(val)
            if nrows == 0
                mode isa Val{:insert} && @assert loc == 1
                newcol = Tables.allocatecolumn(S, targetrows)
            else
                newcol = Tables.allocatecolumn(Union{Missing, S}, targetrows)
                fill!(newcol, missing)
            end
            firstindex(newcol) != 1 && _onebased_check_error()
            mode isa Val{:push} && (newcol[end] = val)
            mode isa Val{:pushfirst} && (newcol[1] = val)
            mode isa Val{:insert} && (newcol[loc] = val)
            df[!, colname] = newcol
        end
        _drop_all_nonnote_metadata!(df)
        return df
    end

    if cols == :orderequal
        if old_row_type <: Dict
            throw(ArgumentError("passing `Dict` as `row` when `cols == :orderequal` " *
                                "is not allowed as it is unordered"))
        elseif length(row) != ncol(df) || any(x -> x[1] != x[2], zip(keys(row), _names(df)))
            throw(ArgumentError("when `cols == :orderequal` pushed row must " *
                                "have the same column names and in the " *
                                "same order as the target data frame"))
        end
    elseif cols === :setequal
        # Only check for equal lengths if :setequal is selected,
        # as an error will be thrown below if some names don't match
        if length(row) != ncols
            # an explicit error is thrown as this was allowed in the past
            throw(ArgumentError("row insertion with `cols` equal to `:setequal` " *
                                "requires `row` to have the same number of elements " *
                                "as the number of columns in `df`."))
        end
    end

    current_col = 0
    try
        for (col, nm) in zip(_columns(df), _names(df))
            current_col += 1
            @assert length(col) == nrows
            if cols === :subset
                val = get(row, nm, missing)
            else
                val = row[nm]
            end
            S = typeof(val)
            T = eltype(col)
            if S <: T || !promote || promote_type(S, T) <: T
                mode isa Val{:push} && push!(col, val)
                mode isa Val{:pushfirst} && pushfirst!(col, val)
                mode isa Val{:insert} && insert!(col, loc, val)
            else
                newcol = similar(col, promote_type(S, T), targetrows)
                firstindex(newcol) != 1 && _onebased_check_error()
                if mode isa Val{:push}
                    copyto!(newcol, 1, col, 1, nrows)
                    newcol[end] = val
                elseif mode isa Val{:pushfirst}
                    newcol[1] = val
                    copyto!(newcol, 2, col, 1, nrows)
                elseif mode isa Val{:insert}
                    copyto!(newcol, 1, col, 1, loc-1)
                    newcol[loc] = val
                    copyto!(newcol, loc+1, col, loc, nrows-loc+1)
                end
                _columns(df)[columnindex(df, nm)] = newcol
            end
        end
    catch err
        @assert current_col > 0
        for j in 1:current_col
            col2 = _columns(df)[j]
            if length(col2) == targetrows
                mode isa Val{:push} && pop!(col2)
                mode isa Val{:pushfirst} && popfirst!(col2)
                mode isa Val{:insert} && deleteat!(col2, loc)
            end
            @assert length(col2) == nrows
        end
        if promote
            @error "Error adding value to column :$(_names(df)[current_col])."
        else
            @error "Error adding value to column :$(_names(df)[current_col]). " *
                   "Maybe you forgot passing `promote=true`?"
        end
        rethrow(err)
    end
    _drop_all_nonnote_metadata!(df)
    return df
end

function Base.push!(df::DataFrame, @nospecialize rows...;
                    cols::Symbol=:setequal,
                    promote::Bool=(cols in [:union, :subset]))
    if !(cols in (:orderequal, :setequal, :intersect, :subset, :union))
        throw(ArgumentError("`cols` keyword argument must be " *
                            ":orderequal, :setequal, :intersect, :subset or :union)"))
    end
    with_names_count = count(rows) do row
        row isa Union{DataFrameRow,AbstractDict,NamedTuple,Tables.AbstractRow}
    end
    if 0 < with_names_count < length(rows)
        throw(ArgumentError("Mixing rows with column names and without column names " *
                            "in a single `push!` call is not allowed"))
    end
    return foldl((df, row) -> push!(df, row, cols=cols, promote=promote), rows, init=df)
end

function Base.pushfirst!(df::DataFrame, @nospecialize rows...;
                         cols::Symbol=:setequal,
                         promote::Bool=(cols in [:union, :subset]))
    if !(cols in (:orderequal, :setequal, :intersect, :subset, :union))
        throw(ArgumentError("`cols` keyword argument must be " *
                            ":orderequal, :setequal, :intersect, :subset or :union)"))
    end
    with_names_count = count(rows) do row
        row isa Union{DataFrameRow,AbstractDict,NamedTuple,Tables.AbstractRow}
    end
    if 0 < with_names_count < length(rows)
        throw(ArgumentError("Mixing rows with column names and without column names " *
                            "in a single `push!` call is not allowed"))
    end
    return foldr((row, df) -> pushfirst!(df, row, cols=cols, promote=promote), rows, init=df)
end
