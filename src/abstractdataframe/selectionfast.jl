"""
    table_transformation(df_sel::AbstractDataFrame, fun)

This is a default function called when `AsTable(...) => fun` is requested. The
`df_sel` argument is a data frame storing columns selected by `AsTable(...)`
selector. By default it calls `default_table_transformation`. However, it is
allowed to add special methods for specific types of `fun` with the reservation
that the produced result must match the result that would be produced by
`default_table_transformation`, except that it is allowed to perform `eltype`
conversion of the resulting vectors or value type promotions that are consistent
with `promote_type`.

It is guaranteed that `df_sel` has at least one column.

The main use of special `table_transformation` methods is to provide more
efficient than the default implementations of requested `fun` transformation.

This function is a part of a public API of DataFrames.jl.

Fast paths are implemented within DataFrames.jl for the following functions `fun`:
* `sum`, `ByRow(sum), `ByRow(sum∘skipmissing)`
* `length`, `length∘skipmissing` (this is not supported in Julia Base, but is a
   convenient way to count number of non-missing entries)
* `mean`, `ByRow(mean), `ByRow(mean∘skipmissing)`
* `minimum`, `ByRow(minimum)`, `ByRow(minimum∘skipmissing)`
* `maximum`, `ByRow(maximum)`, `ByRow(maximum∘skipmissing)`
"""
table_transformation(df_sel::AbstractDataFrame, fun) =
    default_table_transformation(df_sel, fun)

"""
    default_table_transformation(df_sel::AbstractDataFrame, fun)

This is a default implementation called when `AsTable(...) => fun` is requested.
The `df_sel` argument is a data frame storing columns selected by
`AsTable(...)` selector.
"""
@noinline default_table_transformation(df_sel::AbstractDataFrame, fun) =
    fun(Tables.columntable(df_sel))

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

table_transformation(df_sel::AbstractDataFrame, ::ByRow{typeof(mean)}) =
    table_transformation(df_sel, mean)

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
                                      fun::Union{typeof(min), typeof(max)})
    T = mapreduce(eltype, promote_type, cols)
    res = Tables.allocatecolumn(Union{Missing, T}, length(cols[1]))
    fill!(res, missing)
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

# TODO:
# Add these transformations in the future
# - cols => ByRow(coalesce)
# - cols => *
# - AsTable(cols) => prod
# - AsTable(cols) => ByRow(prod)
# - AsTable(cols) => first
# - AsTable(cols) => ByRow(first)
# - AsTable(cols) => ByRow(first∘skipmissing)
# - AsTable(cols) => last
# - AsTable(cols) => ByRow(last)
# - AsTable(cols) => ByRow(last∘skipmissing)
# - AsTable(cols) => var
# - AsTable(cols) => ByRow(var)
# - AsTable(cols) => ByRow(var∘skipmissing)
# - AsTable(cols) => std
# - AsTable(cols) => ByRow(std)
# - AsTable(cols) => ByRow(std∘skipmissing)
