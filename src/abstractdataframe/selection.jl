# normalize_selection function makes sure that whatever input format of idx is it
# will end up in one of four canonical forms
# 1) AbstractVector{Int}
# 2) Pair{Int, <:Pair{<:Base.Callable, <:Union{Symbol, Vector{Symbol}, Type{AsTable}}}}
# 3) Pair{AbstractVector{Int}, <:Pair{<:Base.Callable, <:Union{Symbol, AbstractVector{Symbol}, Type{AsTable}}}}
# 4) Pair{AsTable, <:Pair{<:Base.Callable, <:Union{Symbol, Vector{Symbol}, Type{AsTable}}}}
# 5) Callable

const TRANSFORM_METADATA =
    """
    Metadata: this function propagates table-level `:note`-style metadata.
    Column-level `:note`-style metadata is propagated if:
    a) a single column is transformed to a single column and the name of the column
      does not change (this includes all column selection operations), or
    b) a single column is transformed with `identity` or `copy` to a single column
       even if column name is changed (this includes column renaming).
       As a special case for `GroupedDataFrame` if the output has the same name
       as a grouping column and `keepkeys=true`, metadata is taken from
       original grouping column.
    """

const TRANSFORMATION_COMMON_RULES =
    """
    Below detailed common rules for all transformation functions supported by
    DataFrames.jl are explained and compared.

    All these operations are supported both for
    `AbstractDataFrame` (when split and combine steps are skipped) and
    `GroupedDataFrame`. Technically, `AbstractDataFrame` is just considered as being
    grouped on no columns (meaning it has a single group, or zero groups if it is
    empty). The only difference is that in this case the `keepkeys` and `ungroup`
    keyword arguments (described below) are not supported and a data frame is always
    returned, as there are no split and combine steps in this case.

    In order to perform operations by groups you first need to create a `GroupedDataFrame`
    object from your data frame using the `groupby` function that takes two arguments:
    (1) a data frame to be grouped, and (2) a set of columns to group by.

    Operations can then be applied on each group using one of the following functions:
    * `combine`: does not put restrictions on number of rows returned per group;
      the returned values are vertically concatenated following order of groups in
      `GroupedDataFrame`; it is typically used to compute summary statistics by group;
      for `GroupedDataFrame` if grouping columns are kept they are put as first columns
      in the result;
    * `select`: return a data frame with the number and order of rows exactly the same
      as the source data frame, including only new calculated columns;
      `select!` is an in-place version of `select`; for `GroupedDataFrame` if grouping columns
      are kept they are put as first columns in the result;
    * `transform`: return a data frame with the number and order of rows exactly the same
      as the source data frame, including all columns from the source and new calculated columns;
      `transform!` is an in-place version of `transform`;
      existing columns in the source data frame are put as first columns in the result;

    As a special case, if a `GroupedDataFrame` that has zero groups is passed then
    the result of the operation is determined by performing a single call to the
    transformation function with a 0-row argument passed to it. The output of this
    operation is only used to identify the number and type of produced columns, but
    the result has zero rows.

    All these functions take a specification of one or more functions to apply to
    each subset of the `DataFrame`. This specification can be of the following forms:
    1. standard column selectors (integers, `Symbol`s, strings, vectors of integers,
       vectors of `Symbol`s, vectors of strings,
       `All`, `Cols`, `:`, `Between`, `Not` and regular expressions)
    2. a `cols => function` pair indicating that `function` should be called with
       positional arguments holding columns `cols`, which can be any valid column selector;
       in this case target column name is automatically generated and it is assumed that
       `function` returns a single value or a vector; the generated name is created by
       concatenating source column name and `function` name by default (see examples below).
    3. a `cols => function => target_cols` form additionally explicitly specifying
       the target column or columns, which must be a single name (as a `Symbol` or a string),
       a vector of names or `AsTable`. Additionally it can be a `Function` which
       takes a string or a vector of strings as an argument containing names of columns
       selected by `cols`, and returns the target columns names (all accepted types
       except `AsTable` are allowed).
    4. a `col => target_cols` pair, which renames the column `col` to `target_cols`, which
       must be single name (as a `Symbol` or a string), a vector of names or `AsTable`.
    5. column-independent operations `function => target_cols` or just `function`
       for specific `function`s where the input columns are omitted;
       without `target_cols` the new column has the same name as `function`, otherwise
       it must be single name (as a `Symbol` or a string). Supported `function`s are:
       * `nrow` to efficiently compute the number of rows in each group.
       * `proprow` to efficiently compute the proportion of rows in each group.
       * `eachindex` to return a vector holding the number of each row within each group.
       * `groupindices` to return the group number.
    6. vectors or matrices containing transformations specified by the `Pair` syntax
       described in points 2 to 5
    7. a function which will be called with a `SubDataFrame` corresponding to each group
       if a `GroupedDataFrame` is processed, or with the data frame itself if
       an `AbstractDataFrame` is processed;
       this form should be avoided due to its poor performance unless the number of groups
       is small or a very large number of columns are processed
       (in which case `SubDataFrame` avoids excessive compilation)

    Note! If the expression of the form `x => y` is passed then except for the special
    convenience form `nrow => target_cols` it is always interpreted as
    `cols => function`. In particular the following expression `function => target_cols`
    is not a valid transformation specification.

    Note! If `cols` or `target_cols` are one of `All`, `Cols`, `Between`, or `Not`,
    broadcasting using `.=>` is supported and is equivalent to broadcasting
    the result of `names(df, cols)` or `names(df, target_cols)`.
    This behaves as if broadcasting happened after replacing the selector
    with selected column names within the data frame scope.

    All functions have two types of signatures. One of them takes a `GroupedDataFrame`
    as the first argument and an arbitrary number of transformations described above
    as following arguments. The second type of signature is when a `Function` or a `Type`
    is passed as the first argument and a `GroupedDataFrame` as the second argument
    (similar to `map`).

    As a special rule, with the `cols => function` and `cols => function =>
    target_cols` syntaxes, if `cols` is wrapped in an `AsTable`
    object then a `NamedTuple` containing columns selected by `cols` is passed to
    `function`. The documentation of [`DataFrames.table_transformation`](@ref) provides
    more information about this functionality, in particular covering performance
    considerations.

    What is allowed for `function` to return is determined by the `target_cols` value:
    1. If both `cols` and `target_cols` are omitted (so only a `function` is passed),
       then returning a data frame, a matrix, a `NamedTuple`, a `Tables.AbstractRow`
       or a `DataFrameRow` will
       produce multiple columns in the result. Returning any other value produces
       a single column.
    2. If `target_cols` is a `Symbol` or a string then the function is assumed to return
       a single column. In this case returning a data frame, a matrix, a `NamedTuple`,
       a `Tables.AbstractRow`, or a `DataFrameRow` raises an error.
    3. If `target_cols` is a vector of `Symbol`s or strings or `AsTable` it is assumed
       that `function` returns multiple columns.
       If `function` returns one of `AbstractDataFrame`, `NamedTuple`, `DataFrameRow`,
       `Tables.AbstractRow`, `AbstractMatrix` then rules described in point 1 above apply.
       If `function` returns an `AbstractVector` then each element of this vector must
       support the `keys` function, which must return a collection of `Symbol`s, strings
       or integers; the return value of `keys` must be identical for all elements.
       Then as many columns are created as there are elements in the return value
       of the `keys` function. If `target_cols` is `AsTable` then their names
       are set to be equal to the key names except if `keys` returns integers, in
       which case they are prefixed by `x` (so the column names are e.g. `x1`,
       `x2`, ...). If `target_cols` is a vector of `Symbol`s or strings then
       column names produced using the rules above are ignored and replaced by
       `target_cols` (the number of columns must be the same as the length of
       `target_cols` in this case).
       If `fun` returns a value of any other type then it is assumed that it is a
       table conforming to the Tables.jl API and the `Tables.columntable` function
       is called on it to get the resulting columns and their names. The names are
       retained when `target_cols` is `AsTable` and are replaced if
       `target_cols` is a vector of `Symbol`s or strings.

    In all of these cases, `function` can return either a single row or multiple
    rows. As a particular rule, values wrapped in a `Ref` or a `0`-dimensional
    `AbstractArray` are unwrapped and then treated as a single row.

    `select`/`select!` and `transform`/`transform!` always return a data frame
    with the same number and order of rows as the source (even if `GroupedDataFrame`
    had its groups reordered), except when selection results in zero columns
    in the resulting data frame (in which case the result has zero rows).

    For `combine`, rows in the returned object appear in the order of groups in the
    `GroupedDataFrame`. The functions can return an arbitrary number of rows for
    each group, but the kind of returned object and the number and names of columns
    must be the same for all groups, except when a `DataFrame()` or `NamedTuple()`
    is returned, in which case a given group is skipped.

    It is allowed to mix single values and vectors if multiple transformations
    are requested. In this case single value will be repeated to match the length
    of columns specified by returned vectors.

    To apply `function` to each row instead of whole columns, it can be wrapped in a
    `ByRow` struct. `cols` can be any column indexing syntax, in which case
    `function` will be passed one argument for each of the columns specified by
    `cols` or a `NamedTuple` of them if specified columns are wrapped in `AsTable`.
    If `ByRow` is used it is allowed for `cols` to select an empty set of columns,
    in which case `function` is called for each row without any arguments and an
    empty `NamedTuple` is passed if empty set of columns is wrapped in `AsTable`.

    If a collection of column names is passed then requesting duplicate column
    names in target data frame are accepted (e.g. `select!(df, [:a], :, r"a")`
    is allowed) and only the first occurrence is used. In particular a syntax to
    move column `:col` to the first position in the data frame is
    `select!(df, :col, :)`. On the contrary, output column names of renaming,
    transformation and single column selection operations must be unique, so e.g.
    `select!(df, :a, :a => :a)` or `select!(df, :a, :a => ByRow(sin) => :a)` are not allowed.

    In general columns returned by transformations are stored in the target data
    frame without copying. An exception to this rule is when columns from
    the source data frame are reused in the target data frame.
    This can happen via expressions like: `:x1`, `[:x1, :x2]`, `:x1 => :x2`,
    `:x1 => identity => :x2`, or `:x1 => (x -> @view x[inds])`
    (note that in the last case the source column is reused
    indirectly via a view). In such cases the
    behavior depends on the value of the `copycols` keyword argument:
    * if `copycols=true` then results of such transformations always perform a
      copy of the source column or its view;
    * if `copycols=false` then copies are only performed to avoid storing the
      same column several times in the target data frame; more precisely, no
      copy is made the first time a column is used, but each subsequent
      reuse of a source column (when compared using `===`,
      which excludes views of source columns) performs a copy;

    Note that performing `transform!` or `select!` assumes that `copycols=false`.

    If `df` is a `SubDataFrame` and `copycols=true` then a `DataFrame` is
    returned and the same copying rules apply as for a `DataFrame` input: this
    means in particular that selected columns will be copied. If
    `copycols=false`, a `SubDataFrame` is returned without copying columns and
    in this case transforming or renaming columns is not allowed.

    If a `GroupedDataFrame` is passed and `threads=true` (the default),
    a separate task is spawned for each specified transformation;
    each transformation then spawns as many tasks
    as Julia threads, and splits processing of groups across them
    (however, currently transformations with optimized implementations like `sum`
    and transformations that return multiple rows use a single task for all groups).
    This allows for parallel operation when Julia was started with more than one
    thread. Passed transformation functions must therefore not modify global
    variables (i.e. they must be pure), use locks to control parallel accesses,
    or `threads=false` must be passed to disable multithreading.
    In the future, parallelism may be extended to other cases, so this requirement
    also holds for `DataFrame` inputs.

    In order to improve the performance of the operations some transformations
    invoke optimized implementation, see [`DataFrames.table_transformation`](@ref) for details.
    """

broadcast_pair(df::AbstractDataFrame, @nospecialize(p::Any)) = p

function broadcast_pair(df::AbstractDataFrame, @nospecialize(p::Pair))
    src, second = p
    src_broadcast = src isa Union{InvertedIndices.BroadcastedInvertedIndex,
                                  DataAPI.BroadcastedSelector}
    second_broadcast = second isa Union{InvertedIndices.BroadcastedInvertedIndex,
                                        DataAPI.BroadcastedSelector}
    if second isa Pair
        fun, dst = second
        dst_broadcast = dst isa Union{InvertedIndices.BroadcastedInvertedIndex,
                                      DataAPI.BroadcastedSelector}
        if src_broadcast || dst_broadcast
            new_src = src_broadcast ? names(df, src.sel) : src
            new_dst = dst_broadcast ? names(df, dst.sel) : dst
            new_p = new_src .=> fun .=> new_dst
            return isempty(new_p) ? [] : new_p
        else
            return p
        end
    else
        if src_broadcast || second_broadcast
            new_src = src_broadcast ? names(df, src.sel) : src
            new_second = second_broadcast ? names(df, second.sel) : second
            new_p = new_src .=> new_second
            return isempty(new_p) ? [] : new_p
        else
            return p
        end
    end
end

# this is needed in broadcasting when one of dimensions has length 0
# as then broadcasting produces Matrix{Any} rather than Matrix{<:Pair}
broadcast_pair(df::AbstractDataFrame, @nospecialize(p::AbstractMatrix)) =
    isempty(p) ? [] : p

function broadcast_pair(df::AbstractDataFrame, @nospecialize(p::AbstractVecOrMat{<:Pair}))
    isempty(p) && return []
    need_broadcast = false

    src = first.(p)
    first_src = first(src)
    if first_src isa Union{InvertedIndices.BroadcastedInvertedIndex,
                           DataAPI.BroadcastedSelector}
        if any(!=(first_src), src)
            throw(ArgumentError("when broadcasting column selector it must " *
                                "have a constant value"))
        end
        need_broadcast = true
        new_names = names(df, first_src.sel)
        if !(length(new_names) == size(p, 1) || size(p, 1) == 1)
            throw(ArgumentError("broadcasted dimension does not match the " *
                                "number of selected columns"))
        end
        new_src = new_names
    else
        new_src = src
    end

    second = last.(p)
    first_second = first(second)
    if first_second isa Union{InvertedIndices.BroadcastedInvertedIndex,
                              DataAPI.BroadcastedSelector}
        if any(!=(first_second), second)
            throw(ArgumentError("when using broadcasted column selector it " *
                                "must have a constant value"))
        end
        need_broadcast = true
        new_names = names(df, first_second.sel)
        if !(length(new_names) == size(p, 1) || size(p, 1) == 1)
            throw(ArgumentError("broadcasted dimension does not match the " *
                                "number of selected columns"))
        end
        new_second = new_names
    else
        if first_second isa Pair
            fun, dst = first_second
            if dst isa Union{InvertedIndices.BroadcastedInvertedIndex,
                             DataAPI.BroadcastedSelector}
                if !all(x -> x isa Pair && last(x) == dst, second)
                    throw(ArgumentError("when using broadcasted column selector " *
                                        "it must have a constant value"))
                end
                need_broadcast = true
                new_names = names(df, dst.sel)
                if !(length(new_names) == size(p, 1) || size(p, 1) == 1)
                    throw(ArgumentError("broadcasted dimension does not match the " *
                                        "number of selected columns"))
                end
                new_dst = new_names
                new_second = first.(second) .=> new_dst
            else
                new_second = second
            end
        else
            new_second = second
        end
    end

    if need_broadcast
        new_p = new_src .=> new_second
        return isempty(new_p) ? [] : new_p
    else
        return p
    end
end

# add a method to funname defined in other/utils.jl
funname(row::ByRow) = funname(row.fun)

make_pair_concrete(@nospecialize(x::Pair)) =
    make_pair_concrete(x.first) => make_pair_concrete(x.second)
make_pair_concrete(@nospecialize(x)) = x

normalize_selection(idx::AbstractIndex, @nospecialize(sel), renamecols::Bool) = idx[sel]

normalize_selection(idx::AbstractIndex, @nospecialize(sel::Base.Callable), renamecols::Bool) = sel
normalize_selection(idx::AbstractIndex, sel::Colon, renamecols::Bool) = idx[:]

normalize_selection(idx::AbstractIndex, sel::Pair{typeof(nrow), Symbol},
                    renamecols::Bool) =
    length(idx) == 0 ? (Int[] => (() -> 0) => last(sel)) : (1 => length => last(sel))
normalize_selection(idx::AbstractIndex, sel::Pair{typeof(nrow), <:AbstractString},
                    renamecols::Bool) =
    normalize_selection(idx, first(sel) => Symbol(last(sel)), renamecols)
normalize_selection(idx::AbstractIndex, sel::typeof(nrow), renamecols::Bool) =
    normalize_selection(idx, nrow => :nrow, renamecols)

normalize_selection(idx::AbstractIndex, sel::Pair{typeof(eachindex), Symbol},
                    renamecols::Bool) =
    length(idx) == 0 ? (Int[] => (() -> Base.OneTo(0)) => last(sel)) : (1 => eachindex => last(sel))
normalize_selection(idx::AbstractIndex, sel::Pair{typeof(eachindex), <:AbstractString},
                    renamecols::Bool) =
    normalize_selection(idx, first(sel) => Symbol(last(sel)), renamecols)
normalize_selection(idx::AbstractIndex, sel::typeof(eachindex), renamecols::Bool) =
    normalize_selection(idx, eachindex => :eachindex, renamecols)

normalize_selection(idx::AbstractIndex, sel::Pair{typeof(groupindices), Symbol},
                    renamecols::Bool) =
    Int[] => groupindices => last(sel)
normalize_selection(idx::AbstractIndex, sel::Pair{typeof(groupindices), <:AbstractString},
                    renamecols::Bool) =
    normalize_selection(idx, first(sel) => Symbol(last(sel)), renamecols)
normalize_selection(idx::AbstractIndex, sel::typeof(groupindices), renamecols::Bool) =
    normalize_selection(idx, groupindices => :groupindices, renamecols)

normalize_selection(idx::AbstractIndex, sel::Pair{typeof(proprow), Symbol},
                    renamecols::Bool) =
    Int[] => proprow => last(sel)
normalize_selection(idx::AbstractIndex, sel::Pair{typeof(proprow), <:AbstractString},
                    renamecols::Bool) =
    normalize_selection(idx, first(sel) => Symbol(last(sel)), renamecols)
normalize_selection(idx::AbstractIndex, sel::typeof(proprow), renamecols::Bool) =
    normalize_selection(idx, proprow => :proprow, renamecols)

function normalize_selection(idx::AbstractIndex, sel::ColumnIndex, renamecols::Bool)
    c = idx[sel]
    return c => identity => _names(idx)[c]
end

function normalize_selection(idx::AbstractIndex, sel::Pair{<:ColumnIndex, Symbol},
                             renamecols::Bool)
    c = idx[first(sel)]
    return c => identity => last(sel)
end

normalize_selection(idx::AbstractIndex, sel::Pair{<:ColumnIndex, <:AbstractString},
                    renamecols::Bool) =
    normalize_selection(idx, first(sel) => Symbol(last(sel)), renamecols)

normalize_selection(idx::AbstractIndex, sel::Pair{<:ColumnIndex,
                                                  <:Union{AbstractVector{Symbol},
                                                          AbstractVector{<:AbstractString}}},
                    renamecols::Bool) =
    normalize_selection(idx, first(sel) => identity => last(sel), renamecols)

function normalize_selection(idx::AbstractIndex,
                             @nospecialize(sel::Pair{<:ColumnIndex,
                                                     <:Pair{<:Base.Callable,
                                                            <:Union{Symbol, AbstractString}}}),
                             renamecols::Bool)
    src, (fun, dst) = sel
    return idx[src] => fun => Symbol(dst)
end

function normalize_selection(idx::AbstractIndex,
                             @nospecialize(sel::Pair{<:Any,
                                                     <:Pair{<:Base.Callable,
                                                            <:Union{Symbol, AbstractString, DataType,
                                                                    AbstractVector{Symbol},
                                                                    AbstractVector{<:AbstractString},
                                                                    Function}}}),
                             renamecols::Bool)
    lls = last(last(sel))

    if lls isa DataType
        lls === AsTable || throw(ArgumentError("Only DataType supported as target is AsTable"))
    end

    if first(sel) isa AsTable
        rawc = first(sel).cols
        wanttable = true
    else
        rawc = first(sel)
        wanttable = false
    end

    if rawc isa AbstractVector{Int}
        c = rawc
    elseif rawc isa Union{AbstractVector{Symbol}, AbstractVector{<:AbstractString}}
        c = [idx[n] for n in rawc]
    else
        c = try
                idx[rawc]
            catch e
                if e isa MethodError && e.f === getindex && e.args === (idx, rawc)
                    throw(ArgumentError("Unrecognized column selector: $rawc"))
                else
                    rethrow(e)
                end
            end
    end

    if lls isa Function
        fun_colnames = _names(idx)[c]
        # if AsTable was used as source we always treat it as multicolumn selector
        if wanttable && fun_colnames isa Symbol
            fun_colnames = [fun_colnames]
        end
        lls = lls(string.(fun_colnames))
        if !(lls isa Union{Symbol, AbstractString, AbstractVector{Symbol},
                           AbstractVector{<:AbstractString}})
            throw(ArgumentError("function producing target column names must " *
                                "return a Symbol, a string, a vector of Symbols " *
                                "or a vector of strings"))
        end
    end
    if lls isa AbstractString
        combine_target_col = Symbol(lls)
    elseif lls isa AbstractVector{<:AbstractString}
        combine_target_col = Symbol.(lls)
    else
        combine_target_col = lls
    end

    if combine_target_col isa AbstractVector{Symbol}
        allunique(combine_target_col) || throw(ArgumentError("target column names must be unique"))
    end

    if wanttable
        combine_src = AsTable(c)
    else
        combine_src = (length(c) == 1 ? only(c) : c)
    end

    combine_func = first(last(sel))

    return combine_src => combine_func => combine_target_col
end

function normalize_selection(idx::AbstractIndex,
                             @nospecialize(sel::Pair{<:ColumnIndex, <:Base.Callable}), renamecols::Bool)
    c = idx[first(sel)]
    fun = last(sel)

    if fun === AsTable
        return normalize_selection(idx, first(sel) => identity => AsTable, renamecols)
    end

    if renamecols
        newcol = Symbol(_names(idx)[c], "_", funname(fun))
    else
        newcol = _names(idx)[c]
    end
    return c => fun => newcol
end

function normalize_selection(idx::AbstractIndex,
                             @nospecialize(sel::Pair{<:Any, <:Base.Callable}), renamecols::Bool)
    if first(sel) isa AsTable
        rawc = first(sel).cols
        wanttable = true
    else
        rawc = first(sel)
        wanttable = false
    end
    if rawc isa AbstractVector{Int}
        c = rawc
    elseif rawc isa Union{AbstractVector{Symbol}, AbstractVector{<:AbstractString}}
        c = [idx[n] for n in rawc]
    else
        c = try
                idx[rawc]
            catch e
                if e isa MethodError && e.f === getindex && e.args === (idx, rawc)
                    throw(ArgumentError("Unrecognized column selector: $rawc"))
                else
                    rethrow(e)
                end
            end
    end
    fun = last(sel)

    fun === AsTable && throw(ArgumentError("Passing AsTable in $sel is not supported"))

    if length(c) > 3
        prefix = join(@views(_names(idx)[c[1:2]]), '_')
        if renamecols
            newcol = Symbol(prefix, "_etc_", funname(fun))
        else
            newcol = Symbol(prefix, "_etc")
        end
    elseif isempty(c)
        renamecols || throw(ArgumentError("when renamecols=false target column name " *
                                          "must be passed if there are no input columns"))
        newcol = Symbol(funname(fun))
    else
        prefix = join(view(_names(idx), c), '_')
        if renamecols
            newcol = Symbol(prefix, '_', funname(fun))
        else
            newcol = Symbol(prefix)
        end
    end

    if wanttable
        combine_src = AsTable(c)
    else
        combine_src = (length(c) == 1 ? only(c) : c)
    end

    return combine_src => fun => newcol
end

_transformation_helper(df::AbstractDataFrame, col_idx::Nothing, (fun,)::Ref{Any}) =
    fun(df)
_transformation_helper(df::AbstractDataFrame, col_idx::Int, (fun,)::Ref{Any}) =
    fun(df[!, col_idx])

_empty_astable_helper(fun, len) = [fun(NamedTuple()) for _ in 1:len]

function _transformation_helper(df::AbstractDataFrame, col_idx::AsTable, (fun,)::Ref{Any})
    cols = index(df)[col_idx.cols]
    df_sel = select(df, cols, copycols=false)
    if ncol(df_sel) == 0
        if fun isa ByRow
            # work around fact that length∘skipmissing is not supported in Julia Base yet
            if fun === ByRow(length∘skipmissing)
                return _empty_astable_helper(length, nrow(df))
            else
                return _empty_astable_helper(fun.fun, nrow(df))
            end
        else
            return fun(NamedTuple())
        end
    else
        return table_transformation(df_sel, fun)
    end
end

_empty_selector_helper(fun, len) = [fun() for _ in 1:len]

function _transformation_helper(df::AbstractDataFrame, col_idx::AbstractVector{Int}, (fun,)::Ref{Any})
    if isempty(col_idx)
        if fun isa ByRow
            return _empty_selector_helper(fun.fun, nrow(df))
        else
            return fun()
        end
    else
        cdf = eachcol(df)
        cols = map(c -> cdf[c], col_idx)
        if (fun === +) || fun === ByRow(+) # removing parentheses leads to a parsing error
            return reduce(+, cols)
        elseif fun === ByRow(min)
            return _minmax_row_fast(cols, min)
        elseif fun === ByRow(max)
            return _minmax_row_fast(cols, max)
        else
            return fun(cols...)
        end
    end
end

function _gen_colnames(@nospecialize(res), newname::Union{AbstractVector{Symbol},
                                                          Type{AsTable}, Nothing})
    if res isa AbstractMatrix
        colnames = gennames(size(res, 2))
    elseif res isa Tables.AbstractRow
        colnames = Tables.columnnames(res)
    else
        colnames = propertynames(res)
    end

    if newname !== AsTable && newname !== nothing
        if length(colnames) != length(newname)
            throw(ArgumentError("Number of returned columns is $(length(colnames)) " *
                                "and it does not match the number of provided output " *
                                "names which is $(length(newname))"))
        end
        colnames = newname
    end

    # fix the type to avoid unnecessary compilations of methods
    # this should be cheap
    return colnames isa Vector{Symbol} ? colnames : collect(Symbol, colnames)
end

function _insert_row_multicolumn(newdf::DataFrame, df::AbstractDataFrame,
                                 allow_resizing_newdf::Ref{Bool}, colnames::AbstractVector{Symbol},
                                 @nospecialize(res::Union{NamedTuple, DataFrameRow, Tables.AbstractRow}))
    if ncol(newdf) == 0
        # if allow_resizing_newdf[] is false we know this is select or transform
        rows = allow_resizing_newdf[] ? 1 : nrow(df)
    else
        # allow squashing a scalar to 0 rows
        rows = nrow(newdf)
    end
    @assert length(colnames) == length(res)
    for (newname, v) in zip(colnames, res)
        # note that newdf potentially can contain newname in general
        newdf[!, newname] = fill!(Tables.allocatecolumn(typeof(v), rows), v)
    end
end

function _fix_existing_columns_for_vector(newdf::DataFrame, df::AbstractDataFrame,
                                          allow_resizing_newdf::Ref{Bool}, lr::Int,
                                          (fun,)::Ref{Any})
    # allow shortening to 0 rows
    if allow_resizing_newdf[] && nrow(newdf) == 1
        newdfcols = _columns(newdf)
        for (i, col) in enumerate(newdfcols)
            newcol = fill!(similar(col, lr), first(col))
            firstindex(newcol) != 1 && _onebased_check_error()
            newdfcols[i] = newcol
        end
    end
    # !allow_resizing_newdf[] && ncol(newdf) == 0
    # means that we use `select` or `transform` not `combine`
    if !allow_resizing_newdf[] && ncol(newdf) == 0 && lr != nrow(df)
        throw(ArgumentError("length $(lr) of vector returned from " *
                            "function $fun is different from number of rows " *
                            "$(nrow(df)) of the source data frame."))
    end
    allow_resizing_newdf[] = false
end

function _add_col_check_copy(newdf::DataFrame, df::AbstractDataFrame,
                             col_idx::Union{Nothing, Int, AbstractVector{Int}, AsTable},
                             copycols::Bool, (fun,)::Ref{Any},
                             newname::Symbol, v::AbstractVector,
                             column_to_copy::BitVector)
    cdf = eachcol(df)
    vpar = parent(v)
    parent_cols = col_idx isa AsTable ? col_idx.cols : something(col_idx, 1:ncol(df))
    # note that if col_idx is AsTable thanks to normalization in preprocessing
    # we know that cols is vector of integers here
    @assert parent_cols isa Union{Int, AbstractVector{Int}}
    if copycols
        if !(fun isa ByRow) && (v isa SubArray || any(i -> vpar === parent(cdf[i]), parent_cols))
            newdf[!, newname] = copy(v)
        else
            newdf[!, newname] = v
        end
    else
        if fun isa ByRow
            newdf[!, newname] = v
        else
            must_copy = false
            for i in parent_cols
                # there might be multiple aliases of the same column
                if v === cdf[i]
                    if column_to_copy[i]
                        must_copy = true
                        break
                    else
                        column_to_copy[i] = true
                    end
                end
            end
            newdf[!, newname] = must_copy ? copy(v) : v
        end
    end
end

function _add_multicol_res(res::AbstractDataFrame, newdf::DataFrame, df::AbstractDataFrame,
                           colnames::AbstractVector{Symbol},
                           allow_resizing_newdf::Ref{Bool}, wfun::Ref{Any},
                           col_idx::Union{Nothing, Int, AbstractVector{Int}, AsTable},
                           copycols::Bool, newname::Union{Nothing, Type{AsTable}, AbstractVector{Symbol}},
                           column_to_copy::BitVector)
    lr = nrow(res)
    _fix_existing_columns_for_vector(newdf, df, allow_resizing_newdf, lr, wfun)
    @assert length(colnames) == ncol(res)
    for (newname, v) in zip(colnames, eachcol(res))
        _add_col_check_copy(newdf, df, col_idx, copycols, wfun, newname, v, column_to_copy)
    end
end

function _add_multicol_res(res::AbstractMatrix, newdf::DataFrame, df::AbstractDataFrame,
                           colnames::AbstractVector{Symbol},
                           allow_resizing_newdf::Ref{Bool}, wfun::Ref{Any},
                           col_idx::Union{Nothing, Int, AbstractVector{Int}, AsTable},
                           copycols::Bool, newname::Union{Nothing, Type{AsTable}, AbstractVector{Symbol}},
                           column_to_copy::BitVector)
    lr = size(res, 1)
    _fix_existing_columns_for_vector(newdf, df, allow_resizing_newdf, lr, wfun)
    @assert length(colnames) == size(res, 2)
    for (i, newname) in enumerate(colnames)
        newdf[!, newname] = res[:, i]
    end
end

function _add_multicol_res(@nospecialize(res::NamedTuple{<:Any, <:Tuple{Vararg{AbstractVector}}}),
                           newdf::DataFrame, df::AbstractDataFrame,
                           colnames::AbstractVector{Symbol},
                           allow_resizing_newdf::Ref{Bool}, wfun::Ref{Any},
                           col_idx::Union{Nothing, Int, AbstractVector{Int}, AsTable},
                           copycols::Bool, newname::Union{Nothing, Type{AsTable}, AbstractVector{Symbol}},
                           column_to_copy::BitVector)
    lr = length(res[1])
    _fix_existing_columns_for_vector(newdf, df, allow_resizing_newdf, lr, wfun)
    @assert length(colnames) == length(res)
    for (newname, v) in zip(colnames, res)
        _add_col_check_copy(newdf, df, col_idx, copycols, wfun, newname, v, column_to_copy)
    end
end

function _add_multicol_res(@nospecialize(res::NamedTuple), newdf::DataFrame, df::AbstractDataFrame,
                           colnames::AbstractVector{Symbol},
                           allow_resizing_newdf::Ref{Bool}, wfun::Ref{Any},
                           col_idx::Union{Nothing, Int, AbstractVector{Int}, AsTable},
                           copycols::Bool, newname::Union{Nothing, Type{AsTable}, AbstractVector{Symbol}},
                           column_to_copy::BitVector)
    if any(v -> v isa AbstractVector, res)
        throw(ArgumentError("mixing single values and vectors in a named tuple is not allowed"))
    else
        _insert_row_multicolumn(newdf, df, allow_resizing_newdf, colnames, res)
    end
end

function _add_multicol_res(res::DataFrameRow, newdf::DataFrame, df::AbstractDataFrame,
                           colnames::AbstractVector{Symbol},
                           allow_resizing_newdf::Ref{Bool}, wfun::Ref{Any},
                           col_idx::Union{Nothing, Int, AbstractVector{Int}, AsTable},
                           copycols::Bool, newname::Union{Nothing, Type{AsTable}, AbstractVector{Symbol}},
                           column_to_copy::BitVector)
    _insert_row_multicolumn(newdf, df, allow_resizing_newdf, colnames, res)
end

function _add_multicol_res(res::Tables.AbstractRow, newdf::DataFrame, df::AbstractDataFrame,
                           colnames::AbstractVector{Symbol},
                           allow_resizing_newdf::Ref{Bool}, wfun::Ref{Any},
                           col_idx::Union{Nothing, Int, AbstractVector{Int}, AsTable},
                           copycols::Bool, newname::Union{Nothing, Type{AsTable}, AbstractVector{Symbol}},
                           column_to_copy::BitVector)
    _insert_row_multicolumn(newdf, df, allow_resizing_newdf, colnames, res)
end

function select_transform!((nc,)::Ref{Any}, df::AbstractDataFrame, newdf::DataFrame,
                           transformed_cols::Set{Symbol}, copycols::Bool,
                           allow_resizing_newdf::Ref{Bool},
                           column_to_copy::BitVector)
    @assert nc isa Union{Base.Callable,
                         Pair{<:Union{Int, AbstractVector{Int}, AsTable},
                              <:Pair{<:Base.Callable, <:Union{Symbol, AbstractVector{Symbol}, DataType}}}}
    if nc isa Base.Callable
        col_idx, fun, newname = nothing, nc, nothing
    else
        col_idx, (fun, newname) = nc
    end
    wfun = Ref{Any}(fun)

    if newname isa DataType
        newname === AsTable || throw(ArgumentError("Only DataType supported as target is AsTable"))
    end
    # It is allowed to request a transformation operation into a newname column
    # only once. This is ensured by the logic related to transformed_cols set
    # in _manipulate, therefore in select_transform! such a duplicate should not happen
    res = _transformation_helper(df, col_idx, Ref{Any}(fun))

    if newname === AsTable || newname isa AbstractVector{Symbol}
        if res isa AbstractVector && !isempty(res)
            kp1 = keys(res[1])
            prepend = all(x -> x isa Integer, kp1)
            if !(prepend || all(x -> x isa Symbol, kp1) || all(x -> x isa AbstractString, kp1))
                throw(ArgumentError("keys of the returned elements must be " *
                                    "`Symbol`s, strings or integers"))
            end
            if any(x -> !isequal(keys(x), kp1), res)
                throw(ArgumentError("keys of the returned elements must be identical"))
            end
            newres = DataFrame()
            for n in kp1
                newres[!, prepend ? Symbol("x", n) : Symbol(n)] = [x[n] for x in res]
            end
            res = newres
        elseif !(res isa Union{AbstractDataFrame, NamedTuple, DataFrameRow, AbstractMatrix,
                               Tables.AbstractRow})
            if res isa Union{AbstractVector{Any}, AbstractVector{<:AbstractVector}}
                @assert isempty(res)
                res = DataFrame()
            else
                res = Tables.columntable(res)
            end
        end
    end

    if res isa Union{AbstractDataFrame, NamedTuple, DataFrameRow, AbstractMatrix,
                     Tables.AbstractRow}
        if newname isa Symbol
            throw(ArgumentError("Table returned but a single output column was expected"))
        end
        colnames = _gen_colnames(res, newname)
        isempty(colnames) && return # nothing to do

        if any(in(transformed_cols), colnames)
            throw(ArgumentError("Duplicate column name(s) returned: :" *
                                "$(join(intersect(colnames, transformed_cols), ", :"))"))
        else
            startlen = length(transformed_cols)
            union!(transformed_cols, colnames)
            @assert startlen + length(colnames) == length(transformed_cols)
        end
        _add_multicol_res(res, newdf, df, colnames, allow_resizing_newdf, wfun,
                          col_idx, copycols, newname, column_to_copy)
        if !isempty(colmetadatakeys(newdf))
            for cn_multi in colnames
                emptycolmetadata!(newdf, cn_multi)
            end
        end
    elseif res isa AbstractVector
        if newname === nothing
            newname = :x1
        end
        if newname in transformed_cols
            throw(ArgumentError("duplicate output column name: :$newname"))
        else
            push!(transformed_cols, newname)
        end
        lr = length(res)
        _fix_existing_columns_for_vector(newdf, df, allow_resizing_newdf, lr, wfun)
        _add_col_check_copy(newdf, df, col_idx, copycols, wfun, newname, res, column_to_copy)
        if (col_idx isa Int || (col_idx isa AbstractVector{Int} && length(col_idx) == 1)) &&
           (fun === identity || fun === copy || _names(df)[only(col_idx)] == newname)
           _copy_col_note_metadata!(newdf, newname, df, only(col_idx))
        else
            emptycolmetadata!(newdf, newname)
        end
    else
        if newname === nothing
            newname = :x1
        end
        if newname in transformed_cols
            throw(ArgumentError("duplicate output column name: :$newname"))
        else
            push!(transformed_cols, newname)
        end
        res_unwrap = res isa Union{AbstractArray{<:Any, 0}, Ref} ? res[] : res
        if ncol(newdf) == 0
            # if allow_resizing_newdf[] is false we know this is select or transform
            rows = allow_resizing_newdf[] ? 1 : nrow(df)
        else
            # allow squashing a scalar to 0 rows
            rows = nrow(newdf)
        end
        newdf[!, newname] = fill!(Tables.allocatecolumn(typeof(res_unwrap), rows),
                                  res_unwrap)
        if (col_idx isa Int || (col_idx isa AbstractVector{Int} && length(col_idx) == 1)) &&
           (fun === identity || fun === copy || _names(df)[only(col_idx)] == newname)
           _copy_col_note_metadata!(newdf, newname, df, only(col_idx))
        else
            emptycolmetadata!(newdf, newname)
        end
    end
end

"""
    select!(df::AbstractDataFrame, args...;
            renamecols::Bool=true, threads::Bool=true)
    select!(args::Base.Callable, df::DataFrame;
            renamecols::Bool=true, threads::Bool=true)
    select!(gd::GroupedDataFrame, args...; ungroup::Bool=true,
            renamecols::Bool=true, threads::Bool=true)
    select!(f::Base.Callable, gd::GroupedDataFrame; ungroup::Bool=true,
            renamecols::Bool=true, threads::Bool=true)

Mutate `df` or `gd` in place to retain only columns or transformations specified by `args...` and
return it. The result is guaranteed to have the same number of rows as `df` or
parent of `gd`, except when no columns are selected (in which case the result
has zero rows).

If a `SubDataFrame` or `GroupedDataFrame{SubDataFrame}` is passed, the parent data frame
is updated using columns generated by `args...`, following the same rules as indexing:
- for existing columns filtered-out rows are filled with values present in the
  old columns
- for new columns (which is only allowed if `SubDataFrame` was created with `:`
  as column selector) filtered-out rows are filled with `missing`
- dropped columns (which are only allowed if `SubDataFrame` was created with `:`
  as column selector) are removed
- if `SubDataFrame` was not created with `:` as column selector then `select!`
  is only allowed if the transformations keep exactly the same sequence of column
  names as is in the passed `df`

If a `GroupedDataFrame` is passed then it is updated to reflect the new rows of its updated
parent. If there are independent `GroupedDataFrame` objects constructed using
the same parent data frame they might get corrupt.

$TRANSFORMATION_COMMON_RULES

# Keyword arguments
- `renamecols::Bool=true` : whether in the `cols => function` form automatically generated
  column names should include the name of transformation functions or not.
- `ungroup::Bool=true` : whether the return value of the operation on `gd` should be a data
  frame or a `GroupedDataFrame`.
- `threads::Bool=true` : whether transformations may be run in separate tasks which
  can execute in parallel (possibly being applied to multiple rows or groups at the same time).
  Whether or not tasks are actually spawned and their number are determined automatically.
  Set to `false` if some transformations require serial execution or are not thread-safe.

$TRANSFORM_METADATA

See [`select`](@ref) for examples.
"""
select!(df::DataFrame, @nospecialize(args...);
        renamecols::Bool=true, threads::Bool=true) =
    _replace_columns!(df, select(df, args..., copycols=false,
                                 renamecols=renamecols, threads=threads))

select!(df::SubDataFrame, @nospecialize(args...);
        renamecols::Bool=true, threads::Bool=true) =
    _replace_columns!(df, select(df, args..., copycols=true,
                                 renamecols=renamecols, threads=threads),
                      keep_present=false)

function select!(@nospecialize(arg::Base.Callable), df::AbstractDataFrame;
                 renamecols::Bool=true, threads::Bool=true)
    if arg isa Colon
        throw(ArgumentError("First argument must be a transformation if the second argument is a data frame"))
    end
    return select!(df, arg, threads=threads)
end

"""
    transform!(df::AbstractDataFrame, args...;
               renamecols::Bool=true, threads::Bool=true)
    transform!(args::Callable, df::AbstractDataFrame;
               renamecols::Bool=true, threads::Bool=true)
    transform!(gd::GroupedDataFrame, args...;
               ungroup::Bool=true, renamecols::Bool=true, threads::Bool=true)
    transform!(f::Base.Callable, gd::GroupedDataFrame;
               ungroup::Bool=true, renamecols::Bool=true, threads::Bool=true)

Mutate `df` or `gd` in place to add columns specified by `args...` and return it.
The result is guaranteed to have the same number of rows as `df`.
Equivalent to `select!(df, :, args...)` or `select!(gd, :, args...)`,
except that column renaming performs a copy.

$TRANSFORMATION_COMMON_RULES

# Keyword arguments
- `renamecols::Bool=true` : whether in the `cols => function` form automatically generated
  column names should include the name of transformation functions or not.
- `ungroup::Bool=true` : whether the return value of the operation on `gd` should be a data
  frame or a `GroupedDataFrame`.
- `threads::Bool=true` : whether transformations may be run in separate tasks which
  can execute in parallel (possibly being applied to multiple rows or groups at the same time).
  Whether or not tasks are actually spawned and their number are determined automatically.
  Set to `false` if some transformations require serial execution or are not thread-safe.

$TRANSFORM_METADATA

See [`select`](@ref) for examples.
"""
transform!(df::DataFrame, @nospecialize(args...);
           renamecols::Bool=true, threads::Bool=true) =
    select!(df, :, args..., renamecols=renamecols, threads=threads)

transform!(df::SubDataFrame, @nospecialize(args...); renamecols::Bool=true, threads::Bool=true) =
    _replace_columns!(df, select(df, args..., copycols=true, renamecols=renamecols, threads=threads),
                      keep_present=true)

function transform!(@nospecialize(arg::Base.Callable), df::AbstractDataFrame;
                    renamecols::Bool=true, threads::Bool=true)
    if arg isa Colon
        throw(ArgumentError("First argument must be a transformation if the second argument is a data frame"))
    end
    return transform!(df, arg, threads=threads)
end

"""
    select(df::AbstractDataFrame, args...;
           copycols::Bool=true, renamecols::Bool=true, threads::Bool=true)
    select(args::Callable, df::DataFrame;
           renamecols::Bool=true, threads::Bool=true)
    select(gd::GroupedDataFrame, args...;
           copycols::Bool=true, keepkeys::Bool=true, ungroup::Bool=true,
           renamecols::Bool=true, threads::Bool=true)
    select(f::Base.Callable, gd::GroupedDataFrame;
           copycols::Bool=true, keepkeys::Bool=true, ungroup::Bool=true,
           renamecols::Bool=true, threads::Bool=true)

Create a new data frame that contains columns from `df` or `gd` specified by
`args` and return it. The result is guaranteed to have the same number of rows
as `df`, except when no columns are selected (in which case the result has zero
rows).

$TRANSFORMATION_COMMON_RULES

# Keyword arguments
- `copycols::Bool=true` : whether columns of the source data frame should be copied if
  no transformation is applied to them.
- `renamecols::Bool=true` : whether in the `cols => function` form automatically generated
  column names should include the name of transformation functions or not.
- `keepkeys::Bool=true` : whether grouping columns of `gd` should be kept in the returned
  data frame.
- `ungroup::Bool=true` : whether the return value of the operation on `gd` should be a data
  frame or a `GroupedDataFrame`.
- `threads::Bool=true` : whether transformations may be run in separate tasks which
  can execute in parallel (possibly being applied to multiple rows or groups at the same time).
  Whether or not tasks are actually spawned and their number are determined automatically.
  Set to `false` if some transformations require serial execution or are not thread-safe.

$TRANSFORM_METADATA

# Examples
```jldoctest
julia> df = DataFrame(a=1:3, b=4:6)
3×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      4
   2 │     2      5
   3 │     3      6

julia> select(df, 2)
3×1 DataFrame
 Row │ b
     │ Int64
─────┼───────
   1 │     4
   2 │     5
   3 │     6

julia> select(df, :a => ByRow(sin) => :c, :b)
3×2 DataFrame
 Row │ c         b
     │ Float64   Int64
─────┼─────────────────
   1 │ 0.841471      4
   2 │ 0.909297      5
   3 │ 0.14112       6

julia> select(df, :, [:a, :b] => (a, b) -> a .+ b .- sum(b)/length(b))
3×3 DataFrame
 Row │ a      b      a_b_function
     │ Int64  Int64  Float64
─────┼────────────────────────────
   1 │     1      4           0.0
   2 │     2      5           2.0
   3 │     3      6           4.0

julia> select(df, All() .=> [minimum maximum])
3×4 DataFrame
 Row │ a_minimum  b_minimum  a_maximum  b_maximum
     │ Int64      Int64      Int64      Int64
─────┼────────────────────────────────────────────
   1 │         1          4          3          6
   2 │         1          4          3          6
   3 │         1          4          3          6

julia> using Statistics

julia> select(df, AsTable(:) => ByRow(mean), renamecols=false)
3×1 DataFrame
 Row │ a_b
     │ Float64
─────┼─────────
   1 │     2.5
   2 │     3.5
   3 │     4.5

julia> select(df, AsTable(:) => ByRow(mean) => x -> join(x, "_"))
3×1 DataFrame
 Row │ a_b
     │ Float64
─────┼─────────
   1 │     2.5
   2 │     3.5
   3 │     4.5

julia> select(first, df)
3×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      4
   2 │     1      4
   3 │     1      4

julia> df = DataFrame(a=1:3, b=4:6, c=7:9)
3×3 DataFrame
 Row │ a      b      c
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      4      7
   2 │     2      5      8
   3 │     3      6      9

julia> select(df, AsTable(:) => ByRow(x -> (mean=mean(x), std=std(x))) => :stats,
              AsTable(:) => ByRow(x -> (mean=mean(x), std=std(x))) => AsTable)
3×3 DataFrame
 Row │ stats                    mean     std
     │ NamedTup…                Float64  Float64
─────┼───────────────────────────────────────────
   1 │ (mean = 4.0, std = 3.0)      4.0      3.0
   2 │ (mean = 5.0, std = 3.0)      5.0      3.0
   3 │ (mean = 6.0, std = 3.0)      6.0      3.0

julia> df = DataFrame(a=[1, 1, 1, 2, 2, 1, 1, 2],
                      b=repeat([2, 1], outer=[4]),
                      c=1:8)
8×3 DataFrame
 Row │ a      b      c
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      2      1
   2 │     1      1      2
   3 │     1      2      3
   4 │     2      1      4
   5 │     2      2      5
   6 │     1      1      6
   7 │     1      2      7
   8 │     2      1      8

julia> gd = groupby(df, :a)
GroupedDataFrame with 2 groups based on key: a
First Group (5 rows): a = 1
 Row │ a      b      c
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      2      1
   2 │     1      1      2
   3 │     1      2      3
   4 │     1      1      6
   5 │     1      2      7
⋮
Last Group (3 rows): a = 2
 Row │ a      b      c
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     2      1      4
   2 │     2      2      5
   3 │     2      1      8
```

# specifying a name for target column
```jldoctest
julia> df = DataFrame(a=[1, 1, 1, 2, 2, 1, 1, 2],
                      b=repeat([2, 1], outer=[4]),
                      c=1:8);

julia> gd = groupby(df, :a);

julia> select(gd, :c => (x -> sum(log, x)) => :sum_log_c)
8×2 DataFrame
 Row │ a      sum_log_c
     │ Int64  Float64
─────┼──────────────────
   1 │     1    5.52943
   2 │     1    5.52943
   3 │     1    5.52943
   4 │     2    5.07517
   5 │     2    5.07517
   6 │     1    5.52943
   7 │     1    5.52943
   8 │     2    5.07517

julia> select(gd, [:b, :c] .=> sum) # passing a vector of pairs
8×3 DataFrame
 Row │ a      b_sum  c_sum
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      8     19
   2 │     1      8     19
   3 │     1      8     19
   4 │     2      4     17
   5 │     2      4     17
   6 │     1      8     19
   7 │     1      8     19
   8 │     2      4     17
```

# multiple arguments, renaming and keepkeys
```jldoctest
julia> df = DataFrame(a=[1, 1, 1, 2, 2, 1, 1, 2],
                      b=repeat([2, 1], outer=[4]),
                      c=1:8);

julia> gd = groupby(df, :a);

julia> select(gd, :b => :b1, :c => :c1, [:b, :c] => +, keepkeys=false)
8×3 DataFrame
 Row │ b1     c1     b_c_+
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     2      1      3
   2 │     1      2      3
   3 │     2      3      5
   4 │     1      4      5
   5 │     2      5      7
   6 │     1      6      7
   7 │     2      7      9
   8 │     1      8      9
```

# broadcasting and column expansion
```jldoctest
julia> df = DataFrame(a=[1, 1, 1, 2, 2, 1, 1, 2],
                      b=repeat([2, 1], outer=[4]),
                      c=1:8);

julia> gd = groupby(df, :a);

julia> select(gd, :b, AsTable([:b, :c]) => ByRow(extrema) => [:min, :max])
8×4 DataFrame
 Row │ a      b      min    max
     │ Int64  Int64  Int64  Int64
─────┼────────────────────────────
   1 │     1      2      1      2
   2 │     1      1      1      2
   3 │     1      2      2      3
   4 │     2      1      1      4
   5 │     2      2      2      5
   6 │     1      1      1      6
   7 │     1      2      2      7
   8 │     2      1      1      8

julia> select(gd, :, AsTable(Not(:a)) => sum, renamecols=false)
8×4 DataFrame
 Row │ a      b      c      b_c
     │ Int64  Int64  Int64  Int64
─────┼────────────────────────────
   1 │     1      2      1      3
   2 │     1      1      2      3
   3 │     1      2      3      5
   4 │     2      1      4      5
   5 │     2      2      5      7
   6 │     1      1      6      7
   7 │     1      2      7      9
   8 │     2      1      8      9
```

# column-independent operations
```jldoctest
julia> df = DataFrame(a=[1, 1, 1, 2, 2, 1, 1, 2],
                      b=repeat([2, 1], outer=[4]),
                      c=1:8);

julia> gd = groupby(df, :a);

julia> select(gd, nrow, proprow, groupindices, eachindex)
8×5 DataFrame
 Row │ a      nrow   proprow  groupindices  eachindex
     │ Int64  Int64  Float64  Int64         Int64
─────┼────────────────────────────────────────────────
   1 │     1      5    0.625             1          1
   2 │     1      5    0.625             1          2
   3 │     1      5    0.625             1          3
   4 │     2      3    0.375             2          1
   5 │     2      3    0.375             2          2
   6 │     1      5    0.625             1          4
   7 │     1      5    0.625             1          5
   8 │     2      3    0.375             2          3
```
"""
select(df::AbstractDataFrame, @nospecialize(args...);
       copycols::Bool=true, renamecols::Bool=true, threads::Bool=true) =
    manipulate(df, map(x -> broadcast_pair(df, x), args)...,
               copycols=copycols, keeprows=true, renamecols=renamecols)

function select(@nospecialize(arg::Base.Callable), df::AbstractDataFrame;
                renamecols::Bool=true, threads::Bool=true)
    if arg isa Colon
        throw(ArgumentError("First argument must be a transformation if the second argument is a data frame"))
    end
    return select(df, arg, threads=threads)
end

"""
    transform(df::AbstractDataFrame, args...;
              copycols::Bool=true, renamecols::Bool=true, threads::Bool=true)
    transform(f::Callable, df::DataFrame;
              renamecols::Bool=true, threads::Bool=true)
    transform(gd::GroupedDataFrame, args...;
              copycols::Bool=true, keepkeys::Bool=true, ungroup::Bool=true,
              renamecols::Bool=true, threads::Bool=true)
    transform(f::Base.Callable, gd::GroupedDataFrame;
              copycols::Bool=true, keepkeys::Bool=true, ungroup::Bool=true,
              renamecols::Bool=true, threads::Bool=true)

Create a new data frame that contains columns from `df` or `gd` plus columns
specified by `args` and return it. The result is guaranteed to have the same
number of rows as `df`. Equivalent to `select(df, :, args...)` or `select(gd, :, args...)`.

$TRANSFORMATION_COMMON_RULES

# Keyword arguments
- `copycols::Bool=true` : whether columns of the source data frame should be copied if
  no transformation is applied to them.
- `renamecols::Bool=true` : whether in the `cols => function` form automatically generated
  column names should include the name of transformation functions or not.
- `keepkeys::Bool=true` : whether grouping columns of `gd` should be kept in the returned
  data frame.
- `ungroup::Bool=true` : whether the return value of the operation on `gd` should be a data
  frame or a `GroupedDataFrame`.
- `threads::Bool=true` : whether transformations may be run in separate tasks which
  can execute in parallel (possibly being applied to multiple rows or groups at the same time).
  Whether or not tasks are actually spawned and their number are determined automatically.
  Set to `false` if some transformations require serial execution or are not thread-safe.

Note that when the first argument is a `GroupedDataFrame`, `keepkeys=false`
is needed to be able to return a different value for the grouping column:

$TRANSFORM_METADATA

# Examples
```jldoctest
julia> gdf = groupby(DataFrame(x=1:2), :x)
GroupedDataFrame with 2 groups based on key: x
First Group (1 row): x = 1
 Row │ x
     │ Int64
─────┼───────
   1 │     1
⋮
Last Group (1 row): x = 2
 Row │ x
     │ Int64
─────┼───────
   1 │     2

julia> transform(gdf, x -> (x=10,), keepkeys=false)
2×1 DataFrame
 Row │ x
     │ Int64
─────┼───────
   1 │    10
   2 │    10

julia> transform(gdf, x -> (x=10,), keepkeys=true)
ERROR: ArgumentError: column :x in returned data frame is not equal to grouping key :x
```

See [`select`](@ref) for more examples.
"""
transform(df::AbstractDataFrame, @nospecialize(args...);
          copycols::Bool=true, renamecols::Bool=true, threads::Bool=true) =
    select(df, :, args..., copycols=copycols,
           renamecols=renamecols, threads=threads)

function transform(@nospecialize(arg::Base.Callable), df::AbstractDataFrame;
                   renamecols::Bool=true, threads::Bool=true)
    if arg isa Colon
        throw(ArgumentError("First argument to must be a transformation if the second argument is a data frame"))
    end
    return transform(df, arg, threads=threads)
end

"""
    combine(df::AbstractDataFrame, args...;
            renamecols::Bool=true, threads::Bool=true)
    combine(f::Callable, df::AbstractDataFrame;
            renamecols::Bool=true, threads::Bool=true)
    combine(gd::GroupedDataFrame, args...;
            keepkeys::Bool=true, ungroup::Bool=true,
            renamecols::Bool=true, threads::Bool=true)
    combine(f::Base.Callable, gd::GroupedDataFrame;
            keepkeys::Bool=true, ungroup::Bool=true,
            renamecols::Bool=true, threads::Bool=true)

Create a new data frame that contains columns from `df` or `gd` specified by
`args` and return it. The result can have any number of rows that is determined
by the values returned by passed transformations.

$TRANSFORMATION_COMMON_RULES

# Keyword arguments
- `renamecols::Bool=true` : whether in the `cols => function` form automatically generated
  column names should include the name of transformation functions or not.
- `keepkeys::Bool=true` : whether grouping columns of `gd` should be kept in the returned
  data frame.
- `ungroup::Bool=true` : whether the return value of the operation on `gd` should be a data
  frame or a `GroupedDataFrame`.
- `threads::Bool=true` : whether transformations may be run in separate tasks which
  can execute in parallel (possibly being applied to multiple rows or groups at the same time).
  Whether or not tasks are actually spawned and their number are determined automatically.
  Set to `false` if some transformations require serial execution or are not thread-safe.

$TRANSFORM_METADATA

# Examples
```jldoctest
julia> df = DataFrame(a=1:3, b=4:6)
3×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      4
   2 │     2      5
   3 │     3      6

julia> combine(df, :a => sum, nrow, renamecols=false)
1×2 DataFrame
 Row │ a      nrow
     │ Int64  Int64
─────┼──────────────
   1 │     6      3

julia> combine(df, :a => ByRow(sin) => :c, :b)
3×2 DataFrame
 Row │ c         b
     │ Float64   Int64
─────┼─────────────────
   1 │ 0.841471      4
   2 │ 0.909297      5
   3 │ 0.14112       6

julia> combine(df, :, [:a, :b] => (a, b) -> a .+ b .- sum(b)/length(b))
3×3 DataFrame
 Row │ a      b      a_b_function
     │ Int64  Int64  Float64
─────┼────────────────────────────
   1 │     1      4           0.0
   2 │     2      5           2.0
   3 │     3      6           4.0

julia> combine(df, All() .=> [minimum maximum])
1×4 DataFrame
 Row │ a_minimum  b_minimum  a_maximum  b_maximum
     │ Int64      Int64      Int64      Int64
─────┼────────────────────────────────────────────
   1 │         1          4          3          6

julia> using Statistics

julia> combine(df, AsTable(:) => ByRow(mean), renamecols=false)
3×1 DataFrame
 Row │ a_b
     │ Float64
─────┼─────────
   1 │     2.5
   2 │     3.5
   3 │     4.5

julia> combine(df, AsTable(:) => ByRow(mean) => x -> join(x, "_"))
3×1 DataFrame
 Row │ a_b
     │ Float64
─────┼─────────
   1 │     2.5
   2 │     3.5
   3 │     4.5

julia> combine(first, df)
1×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      4

julia> df = DataFrame(a=1:3, b=4:6, c=7:9)
3×3 DataFrame
 Row │ a      b      c
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      4      7
   2 │     2      5      8
   3 │     3      6      9

julia> combine(df, AsTable(:) => ByRow(x -> (mean=mean(x), std=std(x))) => :stats,
               AsTable(:) => ByRow(x -> (mean=mean(x), std=std(x))) => AsTable)
3×3 DataFrame
 Row │ stats                    mean     std
     │ NamedTup…                Float64  Float64
─────┼───────────────────────────────────────────
   1 │ (mean = 4.0, std = 3.0)      4.0      3.0
   2 │ (mean = 5.0, std = 3.0)      5.0      3.0
   3 │ (mean = 6.0, std = 3.0)      6.0      3.0

julia> df = DataFrame(a=repeat([1, 2, 3, 4], outer=[2]),
                      b=repeat([2, 1], outer=[4]),
                      c=1:8);

julia> gd = groupby(df, :a);

julia> combine(gd, :c => sum, nrow)
4×3 DataFrame
 Row │ a      c_sum  nrow
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      6      2
   2 │     2      8      2
   3 │     3     10      2
   4 │     4     12      2

julia> combine(gd, :c => sum, nrow, ungroup=false)
GroupedDataFrame with 4 groups based on key: a
First Group (1 row): a = 1
 Row │ a      c_sum  nrow
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      6      2
⋮
Last Group (1 row): a = 4
 Row │ a      c_sum  nrow
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     4     12      2

julia> combine(gd) do d # do syntax for the slower variant
           sum(d.c)
       end
4×2 DataFrame
 Row │ a      x1
     │ Int64  Int64
─────┼──────────────
   1 │     1      6
   2 │     2      8
   3 │     3     10
   4 │     4     12

julia> combine(gd, :c => (x -> sum(log, x)) => :sum_log_c) # specifying a name for target column
4×2 DataFrame
 Row │ a      sum_log_c
     │ Int64  Float64
─────┼──────────────────
   1 │     1    1.60944
   2 │     2    2.48491
   3 │     3    3.04452
   4 │     4    3.46574

julia> combine(gd, [:b, :c] .=> sum) # passing a vector of pairs
4×3 DataFrame
 Row │ a      b_sum  c_sum
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      4      6
   2 │     2      2      8
   3 │     3      4     10
   4 │     4      2     12

julia> combine(gd) do sdf # dropping group when DataFrame() is returned
          sdf.c[1] != 1 ? sdf : DataFrame()
       end
6×3 DataFrame
 Row │ a      b      c
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     2      1      2
   2 │     2      1      6
   3 │     3      2      3
   4 │     3      2      7
   5 │     4      1      4
   6 │     4      1      8
```

# auto-splatting, renaming and keepkeys
```jldoctest
julia> df = DataFrame(a=repeat([1, 2, 3, 4], outer=[2]),
                      b=repeat([2, 1], outer=[4]),
                      c=1:8);

julia> gd = groupby(df, :a);

julia> combine(gd, :b => :b1, :c => :c1, [:b, :c] => +, keepkeys=false)
8×3 DataFrame
 Row │ b1     c1     b_c_+
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     2      1      3
   2 │     2      5      7
   3 │     1      2      3
   4 │     1      6      7
   5 │     2      3      5
   6 │     2      7      9
   7 │     1      4      5
   8 │     1      8      9
```

# broadcasting and column expansion
```jldoctest
julia> df = DataFrame(a=repeat([1, 2, 3, 4], outer=[2]),
                      b=repeat([2, 1], outer=[4]),
                      c=1:8);

julia> gd = groupby(df, :a);

julia> combine(gd, :b, AsTable([:b, :c]) => ByRow(extrema) => [:min, :max])
8×4 DataFrame
 Row │ a      b      min    max
     │ Int64  Int64  Int64  Int64
─────┼────────────────────────────
   1 │     1      2      1      2
   2 │     1      2      2      5
   3 │     2      1      1      2
   4 │     2      1      1      6
   5 │     3      2      2      3
   6 │     3      2      2      7
   7 │     4      1      1      4
   8 │     4      1      1      8

julia> combine(gd, [:b, :c] .=> Ref) # preventing vector from being spread across multiple rows
4×3 DataFrame
 Row │ a      b_Ref      c_Ref
     │ Int64  SubArray…  SubArray…
─────┼─────────────────────────────
   1 │     1  [2, 2]     [1, 5]
   2 │     2  [1, 1]     [2, 6]
   3 │     3  [2, 2]     [3, 7]
   4 │     4  [1, 1]     [4, 8]

julia> combine(gd, AsTable(Not(:a)) => Ref) # protecting result
4×2 DataFrame
 Row │ a      b_c_Ref
     │ Int64  NamedTup…
─────┼─────────────────────────────────
   1 │     1  (b = [2, 2], c = [1, 5])
   2 │     2  (b = [1, 1], c = [2, 6])
   3 │     3  (b = [2, 2], c = [3, 7])
   4 │     4  (b = [1, 1], c = [4, 8])

julia> combine(gd, :, AsTable(Not(:a)) => sum, renamecols=false)
8×4 DataFrame
 Row │ a      b      c      b_c
     │ Int64  Int64  Int64  Int64
─────┼────────────────────────────
   1 │     1      2      1      3
   2 │     1      2      5      7
   3 │     2      1      2      3
   4 │     2      1      6      7
   5 │     3      2      3      5
   6 │     3      2      7      9
   7 │     4      1      4      5
   8 │     4      1      8      9
```

# protecting returned vectors from being expanded into multiple rows
```jldoctest
julia> df = DataFrame(a=[1, 1, 2, 2],
                      b=[[1, 2], [2, 3], [3, 4], [4, 5]]);

julia> gd = groupby(df, :a);

julia> combine(gd, :b => Ref∘sum)
2×2 DataFrame
 Row │ a      b_Ref_sum
     │ Int64  Array…
─────┼──────────────────
   1 │     1  [3, 5]
   2 │     2  [7, 9]
```
"""
combine(df::AbstractDataFrame, @nospecialize(args...);
        renamecols::Bool=true, threads::Bool=true) =
    manipulate(df, map(x -> broadcast_pair(df, x), args)...,
               copycols=true, keeprows=false, renamecols=renamecols)

function combine(@nospecialize(arg::Base.Callable), df::AbstractDataFrame;
                 renamecols::Bool=true, threads::Bool=true)
    if arg isa Colon
        throw(ArgumentError("First argument to select! must be a transformation if the second argument is a data frame"))
    end
    return combine(df, arg, threads=threads)
end

combine(@nospecialize(f::Pair), gd::AbstractDataFrame;
        renamecols::Bool=true, threads::Bool=true) =
    throw(ArgumentError("First argument must be a transformation if the second argument is a data frame. " *
                        "You can pass a `Pair` as the second argument of the transformation. If you want the return " *
                        "value to be processed as having multiple columns add `=> AsTable` suffix to the pair."))

function manipulate(df::DataFrame, @nospecialize(cs...); copycols::Bool, keeprows::Bool, renamecols::Bool)
    cs_vec = []
    for v in cs
        if v isa AbstractVecOrMat{<:Pair}
            append!(cs_vec, v)
        else
            push!(cs_vec, v)
        end
    end
    normalized_cs = Any[normalize_selection(index(df), make_pair_concrete(c), renamecols) for c in cs_vec]
    return _manipulate(df, normalized_cs, copycols, keeprows)
end

function _manipulate(df::AbstractDataFrame, normalized_cs::Vector{Any}, copycols::Bool, keeprows::Bool)
    @assert !(df isa SubDataFrame && copycols==false)
    newdf = DataFrame()
    # the role of transformed_cols is the following
    # * make sure that we do not use the same target column name twice in transformations;
    #   note though that it can appear in no-transformation selection like
    #   `select(df, :, :a => ByRow(sin) => :a), where :a is produced both by `:`
    #   and by `:a => ByRow(sin) => :a`
    # * make sure that if some column is produced by transformation like
    #   `:a => ByRow(sin) => :a` and it appears earlier or later in non-transforming
    #   selection like `:` or `:a` then the transformation is computed and inserted
    #   in to the target data frame once and only once the first time the target column
    #   is requested to be produced.
    #
    # For example in:
    #
    # julia> df = DataFrame(a=1:2, b=3:4)
    # 2×2 DataFrame
    #  Row │ a      b
    #      │ Int64  Int64
    # ─────┼──────────────
    #    1 │     1      3
    #    2 │     2      4
    #
    # julia> select(df, :, :a => ByRow(sin) => :a)
    # 2×2 DataFrame
    #  Row │ a         b
    #      │ Float64   Int64
    # ─────┼─────────────────
    #    1 │ 0.841471      3
    #    2 │ 0.909297      4
    #
    # julia> select(df, :, :a => ByRow(sin) => :a, :a)
    # ERROR: ArgumentError: duplicate output column name: :a
    #
    # transformed_cols keeps a set of columns that were generated via a transformation
    # up till the point. Note that single column selection and column renaming is
    # considered to be a transformation
    transformed_cols = Set{Symbol}()
    # we allow resizing newdf only if up to some point only scalars were put
    # in it. The moment we put any vector into newdf its number of rows becomes fixed
    # Also if keeprows is true then we make sure to produce nrow(df) rows so resizing
    # is not allowed
    allow_resizing_newdf = Ref(!keeprows)
    # keep track of the fact if single column transformation like
    # :x or :x => :y or :x => identity
    # should make a copy
    # this ensures that we store a column from a source data frame in a
    # destination data frame without copying at most once
    column_to_copy = copycols ? trues(ncol(df)) : falses(ncol(df))

    for nc in normalized_cs
        if nc isa AbstractVector{Int} # only this case is NOT considered to be a transformation
            allunique(nc) || throw(ArgumentError("duplicate column names selected"))
            for i in nc
                newname = _names(df)[i]
                # as nc is a multiple column selection without transformations
                # we allow duplicate column names with selections applied earlier
                # and ignore them for convenience, to allow for e.g. select(df, :x1, :)
                if !hasproperty(newdf, newname)
                    # allow shortening to 0 rows
                    if allow_resizing_newdf[] && nrow(newdf) == 1
                        newdfcols = _columns(newdf)
                        for (i, col) in enumerate(newdfcols)
                            newcol = fill!(similar(col, nrow(df)), first(col))
                            firstindex(newcol) != 1 && _onebased_check_error()
                            newdfcols[i] = newcol
                        end
                    end
                    # here even if keeprows is true all is OK
                    newdf[!, newname] = column_to_copy[i] ? df[:, i] : df[!, i]
                    column_to_copy[i] = true
                    allow_resizing_newdf[] = false
                    _copy_col_note_metadata!(newdf, newname, df, i)
                end
            end
        else
            select_transform!(Ref{Any}(nc), df, newdf, transformed_cols, copycols,
                              allow_resizing_newdf, column_to_copy)
        end
    end
    _copy_table_note_metadata!(newdf, df)
    return newdf
end

function manipulate(dfv::SubDataFrame, @nospecialize(args...); copycols::Bool, keeprows::Bool,
                    renamecols::Bool)
    if copycols
        cs_vec = []
        for v in args
            if v isa AbstractVecOrMat{<:Pair}
                append!(cs_vec, v)
            else
                push!(cs_vec, v)
            end
        end
        normalized_cs = Any[normalize_selection(index(dfv), make_pair_concrete(c), renamecols) for c in cs_vec]
        return _manipulate(dfv, normalized_cs, true, keeprows)
    else
        # we do not support transformations here
        # newinds contains only indexing; making it Vector{Any} avoids some compilation
        newinds = []
        seen_single_column = Set{Int}()
        for ind in args
            if ind isa ColumnIndex
                ind_idx = index(dfv)[ind]
                if ind_idx in seen_single_column
                    throw(ArgumentError("selecting the same column multiple times " *
                                        "using Symbol, string or integer is not allowed " *
                                        "($ind was passed more than once"))
                else
                    push!(seen_single_column, ind_idx)
                end
            else
                newind = index(dfv)[ind]
                push!(newinds, newind)
            end
        end
        return view(dfv, :, Cols(newinds...))
    end
end

function manipulate(df::DataFrame, args::AbstractVector{Int};
                    copycols::Bool, keeprows::Bool, renamecols::Bool)
    new_df = DataFrame(_columns(df)[args], Index(_names(df)[args]), copycols=copycols)
    _copy_all_note_metadata!(new_df, df)
    return new_df
end

function manipulate(df::DataFrame, c::MultiColumnIndex; copycols::Bool, keeprows::Bool,
                    renamecols::Bool)
    if c isa AbstractVector{<:Pair}
        return manipulate(df, c..., copycols=copycols, keeprows=keeprows,
                          renamecols=renamecols)
    else
        return manipulate(df, index(df)[c], copycols=copycols, keeprows=keeprows,
                          renamecols=renamecols)
    end
end

function manipulate(dfv::SubDataFrame, args::MultiColumnIndex;
                    copycols::Bool, keeprows::Bool, renamecols::Bool)
    if args isa AbstractVector{<:Pair}
        return manipulate(dfv, args..., copycols=copycols, keeprows=keeprows,
                          renamecols=renamecols)
    else
        return copycols ? dfv[:, args] : view(dfv, :, args)
    end
end

manipulate(df::DataFrame, c::ColumnIndex; copycols::Bool, keeprows::Bool,
           renamecols::Bool) =
    manipulate(df, Int[index(df)[c]], copycols=copycols, keeprows=keeprows, renamecols=renamecols)

manipulate(dfv::SubDataFrame, c::ColumnIndex; copycols::Bool, keeprows::Bool,
           renamecols::Bool) =
    manipulate(dfv, Int[index(dfv)[c]], copycols=copycols, keeprows=keeprows, renamecols=renamecols)
