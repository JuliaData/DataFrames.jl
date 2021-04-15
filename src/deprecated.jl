export by, aggregate

by(args...; kwargs...) = throw(ArgumentError("by function was removed from DataFrames.jl. " *
                                             "Use the `combine(groupby(...), ...)` or `combine(f, groupby(...))` instead."))

aggregate(args...; kwargs...) = throw(ArgumentError("aggregate function was removed from DataFrames.jl. " *
                                                    "Use the `combine` function instead."))
