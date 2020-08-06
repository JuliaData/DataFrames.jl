import Base: @deprecate

@deprecate DataFrame!(args...; kwargs...) DataFrame(args...; copycols=false, kwargs...)

# TODO: remove these definitions in year 2021
by(args...; kwargs...) = throw(ArgumentError("by function was removed from DataFrames.jl. " *
                                             "Use the `groupby` and `combine` functions instead."))

aggregate(args...; kwargs...) = throw(ArgumentError("aggregate function was removed from DataFrames.jl. " *
                                                    "Use the `combine` functions instead."))
