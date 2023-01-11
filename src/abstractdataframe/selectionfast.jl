"""
    table_transformation(df_sel::AbstractDataFrame, fun)

This is the function called when `AsTable(...) => fun` is requested. The
`df_sel` argument is a data frame storing columns selected by the `AsTable(...)`
selector.

By default it calls `default_table_transformation`. However, it is
allowed to add special methods for specific types of `fun`, as long as
the result matches what would be produced by
`default_table_transformation`, except that it is allowed to perform `eltype`
conversion of the resulting vectors or value type promotions that are consistent
with `promote_type`.

It is guaranteed that `df_sel` has at least one column.

The main use of special `table_transformation` methods is to provide more
efficient than the default implementations of requested `fun` transformation.

This function might become a part of the public API of DataFrames.jl in the
future, currently it should be considered experimental.

Fast paths are implemented within DataFrames.jl for the following functions `fun`:
* `sum`, `ByRow(sum)`, `ByRow(sum∘skipmissing)`
* `length`, `ByRow(length)`, `ByRow(length∘skipmissing)`
* `mean`, `ByRow(mean)`, `ByRow(mean∘skipmissing)`
* `ByRow(var)`, `ByRow(var∘skipmissing)`
* `ByRow(std)`, `ByRow(std∘skipmissing)`
* `ByRow(median)`, `ByRow(median∘skipmissing)`
* `minimum`, `ByRow(minimum)`, `ByRow(minimum∘skipmissing)`
* `maximum`, `ByRow(maximum)`, `ByRow(maximum∘skipmissing)`
* `fun∘collect` and `ByRow(fun∘collect)` where `fun` is any function

Note that in order to improve the performance `ByRow(sum)`,
`ByRow(sum∘skipmissing)`, `ByRow(mean)`, and `ByRow(mean∘skipmissing)`
perform all operations in the target element type. In some very rare cases
(like mixing very large `Int64` values and `Float64` values)
it can lead to a result different from the one that would
be obtained by calling the function outside of DataFrames.jl. The way to
avoid this precision loss is to use an anonymous function, e.g. instead of
`ByRow(sum)` use `ByRow(x -> sum(x))`. However, in general for such
scenarios even standard aggregation functions should not be considered to
provide reliable output, and users are recommended to switch to higher precision
calculations. An example of a case when standard `sum` is affected by the
situation discussed is:
```
julia> sum(Any[typemax(Int), typemax(Int), 1.0])
-1.0

julia> sum(Any[1.0, typemax(Int), typemax(Int)])
1.8446744073709552e19
```
"""
table_transformation(df_sel::AbstractDataFrame, fun) =
    default_table_transformation(df_sel, fun)

"""
    isreadonly(fun)

Trait returning a `Bool` indicator if function `fun` is only reading the passed
argument. Such a function guarantees not to modify nor return in any form the
passed argument. By default `false` is returned.

This function might become a part of the public API of DataFrames.jl in the
future, currently it should be considered experimental. Adding a method to
`isreadonly` for a specific function `fun` will improve performance of
`AsTable(...) => ByRow(fun∘collect)` operation.
"""
isreadonly(::Any) = false
isreadonly(::typeof(sum)) = true
isreadonly(::typeof(sum∘skipmissing)) = true
isreadonly(::typeof(length)) = true
isreadonly(::typeof(mean)) = true
isreadonly(::typeof(mean∘skipmissing)) = true
isreadonly(::typeof(var)) = true
isreadonly(::typeof(var∘skipmissing)) = true
isreadonly(::typeof(std)) = true
isreadonly(::typeof(std∘skipmissing)) = true
isreadonly(::typeof(median)) = true
isreadonly(::typeof(median∘skipmissing)) = true
isreadonly(::typeof(minimum)) = true
isreadonly(::typeof(minimum∘skipmissing)) = true
isreadonly(::typeof(maximum)) = true
isreadonly(::typeof(maximum∘skipmissing)) = true
isreadonly(::typeof(prod)) = true
isreadonly(::typeof(prod∘skipmissing)) = true
isreadonly(::typeof(first)) = true
isreadonly(::typeof(first∘skipmissing)) = true
isreadonly(::typeof(last)) = true

"""
    default_table_transformation(df_sel::AbstractDataFrame, fun)

This is a default implementation called when `AsTable(...) => fun` is requested.
The `df_sel` argument is a data frame storing columns selected by
`AsTable(...)` selector.
"""
function default_table_transformation(df_sel::AbstractDataFrame, fun)
    if fun isa ByRow && fun.fun isa ComposedFunction{<:Any, typeof(collect)}
        vT = unique(typeof.(eachcol(df_sel)))
        if length(vT) == 1 # homogeneous type
            T = eltype(vT[1])
            cT = vT[1]
        elseif length(vT) == 2 # small union
            # Base.promote_typejoin is used wen collecting NamedTuple elements
            T = Base.promote_typejoin(eltype(vT[1]), eltype(vT[2]))
            cT = Union{vT[1], vT[2]}
        else # large union
            # use Base.promote_typejoin to make sure that in case all columns
            # have <:Real eltype the v vector has <:Real eltype which
            # is required by some functions, eg. in StatsBase.jl
            T = mapreduce(eltype, Base.promote_typejoin, vT)
            # use Any for cT, as the common type of columns will be abstract
            # anyway, so it is better to use Any as it should reduce compilation latency
            cT = Any
        end
        v = Vector{T}(undef, ncol(df_sel))
        cols = collect(cT, eachcol(df_sel))
        readonly = isreadonly(fun.fun.outer)
        return _fast_row_aggregate_collect(fun.fun.outer, v, cols, readonly)
    elseif fun isa ComposedFunction{<:Any, typeof(collect)}
        # this will narrow down eltype of the resulting vector
        # but will not perform conversion
        return fun(map(identity, eachcol(df_sel)))
    else
        return fun(Tables.columntable(df_sel))
    end
end

function _populate_v!(v::Vector, cols::Vector, len::Int, i::Int, readonly::Bool)
    for j in 1:len
        @inbounds v[j] = cols[j][i]
    end
    return readonly ? v : copy(v)
end

function _fast_row_aggregate_collect(fun, v::Vector, cols::Vector, readonly::Bool)
    len = length(v)
    n = length(cols[1])
    @assert all(x -> length(x) == n, cols)
    return [fun(_populate_v!(v, cols, len, i, readonly)) for i in 1:n]
end

# this is slower than _sum_fast below, but is required if we want
# to produce the same results as we would without using fast path
# due to the differences in the implementation in Julia Base of sum aggregation
table_transformation(df_sel::AbstractDataFrame, ::typeof(sum)) =
    sum(map(identity, eachcol(df_sel)))

function table_transformation(df_sel::AbstractDataFrame, fun::ByRow{typeof(sum)})
    fastsum = _sum_fast(map(identity, eachcol(df_sel)))
    isnothing(fastsum) || return fastsum
    slowsum = default_table_transformation(df_sel, fun)
    isconcretetype(nonmissingtype(eltype(slowsum))) && return slowsum
    T = mapreduce(typeof, promote_type, slowsum)
    return convert(AbstractVector{T}, slowsum)
end

function _sum_fast(cols::Vector{<:AbstractVector})
    local sumz
    hadmissing = false
    sumz_undefined = true
    for col in cols
        try
            ec = eltype(col)
            if ec >: Missing
                hadmissing = true
            end
            zec = zero(ec)
            zi = Base.add_sum(zec, zec)
            if sumz_undefined
                sumz_undefined = false
                sumz = zi
            else
                sumz = Base.add_sum(sumz, zi)
            end
        catch e
            if e isa MethodError && e.f === zero
                sumz_undefined = true
                break
            else
                throw(e)
            end
        end
    end
    # this will happen if eltype of some columns do not support zero
    sumz_undefined && return nothing
    if hadmissing
        Tres = Union{Missing, typeof(sumz)}
    else
        Tres = typeof(sumz)
    end

    Tres === Missing && return missings(length(cols[1]))

    res = fill!(Tables.allocatecolumn(Tres, length(cols[1])), sumz)

    for (i, col) in enumerate(cols)
        if i == 1
            res .= col
        else
            res .= Base.add_sum.(res, col)
        end
    end

    return res
end

function table_transformation(df_sel::AbstractDataFrame,
                              fun::typeof(ByRow(sum∘skipmissing)))
    fastsum = _sum_skipmissing_fast(map(identity, eachcol(df_sel)))
    isnothing(fastsum) || return fastsum
    slowsum = default_table_transformation(df_sel, fun)
    isconcretetype(nonmissingtype(eltype(slowsum))) && return slowsum
    T = mapreduce(typeof, promote_type, slowsum)
    return convert(AbstractVector{T}, slowsum)
end

function _sum_skipmissing_fast(cols::Vector{<:AbstractVector})
    local sumz
    sumz_undefined = true
    for col in cols
        try
            zec = zero(eltype(col))
            zi = Base.add_sum(zec, zec)
            if !ismissing(zi)
                if sumz_undefined
                    sumz_undefined = false
                    sumz = zi
                else
                    sumz = Base.add_sum(sumz, zi)
                end
            end
        catch e
            if e isa MethodError && e.f === zero
                sumz_undefined = true
                break
            else
                throw(e)
            end
        end
    end
    # this will happen if eltype of some columns do not support zero
    # or all columns have eltype Missing
    sumz_undefined && return nothing
    res = fill!(Tables.allocatecolumn(typeof(sumz), length(cols[1])), sumz)

    for col in cols
        res .= ifelse.(ismissing.(col), res, Base.add_sum.(res, col))
    end

    return res
end

table_transformation(df_sel::AbstractDataFrame, ::typeof(length)) = ncol(df_sel)

table_transformation(df_sel::AbstractDataFrame, ::ByRow{typeof(length)}) =
    fill(ncol(df_sel), nrow(df_sel))

table_transformation(df_sel::AbstractDataFrame,
                     ::ByRow{typeof(length∘skipmissing)}) =
    _length_skipmissing_fast(map(identity, eachcol(df_sel)))

function _length_skipmissing_fast(cols::Vector{<:AbstractVector})
    len = fill(length(cols), length(cols[1]))
    for col in cols
        (Missing <: eltype(col)) && (len .-= ismissing.(col))
    end
    return len
end

table_transformation(df_sel::AbstractDataFrame, ::typeof(mean)) =
    mean(map(identity, eachcol(df_sel)))

function table_transformation(df_sel::AbstractDataFrame, fun::ByRow{typeof(mean)})
    fastmean = _mean_fast(map(identity, eachcol(df_sel)))
    isnothing(fastmean) || return fastmean
    slowmean = default_table_transformation(df_sel, fun)
    isconcretetype(nonmissingtype(eltype(slowmean))) && return slowmean
    T = mapreduce(typeof, promote_type, slowmean)
    return convert(AbstractVector{T}, slowmean)
end

function _mean_fast(cols::Vector{<:AbstractVector})
    local sumz
    hadmissing = false
    sumz_undefined = true
    for col in cols
        try
            ec = eltype(col)
            if ec >: Missing
                hadmissing = true
            end
            zi = zero(ec) / 1
            if sumz_undefined
                sumz_undefined = false
                sumz = zi
            else
                sumz += zi
            end
        catch e
            if e isa MethodError && e.f === zero
                sumz_undefined = true
                break
            else
                throw(e)
            end
        end
    end
    # this will happen if eltype of some columns do not support zero
    sumz_undefined && return nothing
    if hadmissing
        Tres = Union{Missing, typeof(sumz)}
    else
        Tres = typeof(sumz)
    end

    Tres === Missing && return missings(length(cols[1]))

    res = fill!(Tables.allocatecolumn(Tres, length(cols[1])), sumz)

    for (i, col) in enumerate(cols)
        if i == 1
            res .= col
        else
            res .+= col
        end
    end
    res ./= length(cols)
    return res
end

function table_transformation(df_sel::AbstractDataFrame,
                              fun::typeof(ByRow(mean∘skipmissing)))
    fastmean = _mean_skipmissing_fast(map(identity, eachcol(df_sel)))
    isnothing(fastmean) || return fastmean
    slowmean = default_table_transformation(df_sel, fun)
    isconcretetype(nonmissingtype(eltype(slowmean))) && return slowmean
    T = mapreduce(typeof, promote_type, slowmean)
    return convert(AbstractVector{T}, slowmean)
end

function _mean_skipmissing_fast(cols::Vector{<:AbstractVector})
    local sumz
    sumz_undefined = true
    for col in cols
        T = nonmissingtype(eltype(col))
        T === Union{} && continue
        # if T is not concrete we cannot reliably implement fast path
        isconcretetype(T) || return nothing
        try
            zi = zero(T) / 1
            if sumz_undefined
                sumz_undefined = false
                sumz = zi
            else
                sumz += zi
            end
        catch e
            if e isa MethodError && e.f === zero
                sumz_undefined = true
                break
            else
                throw(e)
            end
        end
    end
    # this will happen if eltype of some columns do not support zero
    # or all columns have eltype Missing
    sumz_undefined && return nothing
    sumv = fill!(Tables.allocatecolumn(typeof(sumz), length(cols[1])), sumz)
    lenv = zeros(Int, length(sumv))

    for col in cols
        sumv .= ifelse.(ismissing.(col), sumv, sumv .+ col)
        lenv .+= .!ismissing.(col)
    end
    sumv ./= lenv
    return sumv
end

table_transformation(df_sel::AbstractDataFrame, ::typeof(ByRow(minimum))) =
    _minmax_row_fast(map(identity, eachcol(df_sel)), min)

table_transformation(df_sel::AbstractDataFrame, ::typeof(ByRow(maximum))) =
    _minmax_row_fast(map(identity, eachcol(df_sel)), max)

function _minmax_row_fast(cols::Vector{<:AbstractVector},
                          fun::Union{typeof(min), typeof(max)})
    T = mapreduce(eltype, promote_type, cols)
    res = Tables.allocatecolumn(T, length(cols[1]))
    res .= cols[1]
    for i in 2:length(cols)
        res .= fun.(res, cols[i])
    end
    return res
end

table_transformation(df_sel::AbstractDataFrame, ::typeof(ByRow(minimum∘skipmissing))) =
    _minmax_row_fast_skipmissing(map(identity, eachcol(df_sel)), _min_missing)

table_transformation(df_sel::AbstractDataFrame, ::typeof(ByRow(maximum∘skipmissing))) =
    _minmax_row_fast_skipmissing(map(identity, eachcol(df_sel)), _max_missing)

function _min_missing(a, b)
    ismissing(b) && return a
    ismissing(a) && return b
    return min(a, b)
end

function _max_missing(a, b)
    ismissing(b) && return a
    ismissing(a) && return b
    return max(a, b)
end

function _minmax_row_fast_skipmissing(cols::Vector{<:AbstractVector},
                                      fun::Union{typeof(_min_missing),
                                                 typeof(_max_missing)})
    T = mapreduce(eltype, promote_type, cols)
    res = Tables.allocatecolumn(Union{Missing, T}, length(cols[1]))
    res .= cols[1]
    for i in 2:length(cols)
        res .= fun.(res, cols[i])
    end
    if any(ismissing, res)
        throw(ArgumentError("some rows of selected columns contained only missing values"))
    end
    return disallowmissing(res)
end

function table_transformation(df_sel::AbstractDataFrame, ::typeof(minimum))
    return reduce(min, map(identity, eachcol(df_sel)))
end

function table_transformation(df_sel::AbstractDataFrame, ::typeof(maximum))
    return reduce(max, map(identity, eachcol(df_sel)))
end

table_transformation(df_sel::AbstractDataFrame, ::typeof(ByRow(std))) =
    table_transformation(df_sel, ByRow(std∘collect))
table_transformation(df_sel::AbstractDataFrame, ::typeof(ByRow(std∘skipmissing))) =
    table_transformation(df_sel, ByRow(std∘skipmissing∘collect))

table_transformation(df_sel::AbstractDataFrame, ::typeof(ByRow(var))) =
    table_transformation(df_sel, ByRow(var∘collect))
table_transformation(df_sel::AbstractDataFrame, ::typeof(ByRow(var∘skipmissing))) =
    table_transformation(df_sel, ByRow(var∘skipmissing∘collect))

table_transformation(df_sel::AbstractDataFrame, ::typeof(ByRow(median))) =
    table_transformation(df_sel, ByRow(median!∘collect))
table_transformation(df_sel::AbstractDataFrame, ::typeof(ByRow(median∘skipmissing))) =
    table_transformation(df_sel, ByRow(median∘skipmissing∘collect))
