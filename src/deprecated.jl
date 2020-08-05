import Base: @deprecate

@deprecate DataFrame!(args...; kwargs...) DataFrame(args...; copycols=false, kwargs...)
