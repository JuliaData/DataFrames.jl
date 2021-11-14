export by, aggregate

# TODO: remove definitions in 2.0 release
by(args...; kwargs...) = throw(ArgumentError("by function was removed from DataFrames.jl. " *
                                             "Use the `combine(groupby(...), ...)` or `combine(f, groupby(...))` instead."))
aggregate(args...; kwargs...) = throw(ArgumentError("aggregate function was removed from DataFrames.jl. " *
                                                    "Use the `combine` function instead."))

# TODO: remove deprecation in 2.0 release
import Base.delete!
@deprecate delete!(df::DataFrame, inds) deleteat!(df::DataFrame, inds)