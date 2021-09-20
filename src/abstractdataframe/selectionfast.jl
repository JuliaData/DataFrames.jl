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

The main use of special `table_transformation` methods is to provide more
efficient than the default implementations of requested `fun` transformation.
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

table_transformation(df_sel::AbstractDataFrame, ::typeof(sum)) =
    _sum_fast(map(identity, eachcol(df_sel)))

table_transformation(df_sel::AbstractDataFrame, ::ByRow{typeof(sum)}) =
    table_transformation(df_sel, sum)

function _sum_fast(cols::Vector{<:AbstractVector})
    isempty(cols) && throw(ArgumentError("No columns selected for reduction"))
    return sum(cols)
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
    @assert !isempty(cols)
    local sumz
    sumz_undefined = true
    for col in cols
        try
            zec = zero(eltype(col))
            zi = Base.add_sum(zec, zec)
            if sumz_undefined
                sumz_undefined = false
                sumz = zi
            elseif !ismissing(zi) # zi is missing if eltype is Missing
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
    # or all columns have eltype Missing
    sumz_undefined && return nothing
    init = fill!(Tables.allocatecolumn(typeof(sumz), length(cols[1])), sumz)

    return foldl(cols, init=init) do l, r
        l .= ifelse.(ismissing.(r), l, l .+ r)
    end
end

function table_transformation(df_sel::AbstractDataFrame, ::ByRow{typeof(length)})
    @assert ncol(df) > 0
    return fill(ncol(df_sel), nrow(df_sel))
end

table_transformation(df_sel::AbstractDataFrame,
                     ::ByRow{typeof(length∘skipmissing)}) =
    _length_skipmissing_fast(map(identity, eachcol(df_sel)))

function _length_skipmissing_fast(cols::Vector{<:AbstractVector})
    @assert !isempty(cols)
    len = fill(length(cols), length(cols[1]))
    for col in cols
        (Missing <: eltype(col)) && (len .-= ismissing.(col))
    end
    return len
end

function table_transformation(df_sel::AbstractDataFrame, ::typeof(mean))
    @assert ncol(df_sel) > 0
    return mean(map(identity, eachcol(df_sel)))
end

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
    @assert !isempty(cols)
    local sumz
    sumz_undefined = true
    for col in cols
        try
            zi = zero(eltype(col))
            if sumz_undefined
                sumz_undefined = false
                sumz = zi
            elseif !ismissing(zi) # zi is missing if eltype is Missing
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
    sumv = fill!(Tables.allocatecolumn(typeof(sumz / 0), length(cols[1])), sumz)
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
    isempty(cols) && throw(ArgumentError("No columns selected for reduction"))
    T = mapreduce(typeof, promote_type, cols)
    res = Tables.allocatecolumn(T, length(cols[1]))
    res .= cols[1]
    for i in 2:length(cols)
        res .= fun.(res, cols[i])
    end
    return res
end

function table_transformation(df_sel::AbstractDataFrame, ::typeof(minimum))
    @assert ncol(df_sel) > 0
    return reduce(min, map(identity, eachcol(df_sel)))
end

function table_transformation(df_sel::AbstractDataFrame, ::typeof(maximum))
    @assert ncol(df_sel) > 0
    return reduce(max, map(identity, eachcol(df_sel)))
end
