# TODO: remove deprecation in 2.0 release
import Base.delete!
@deprecate delete!(df::DataFrame, inds) deleteat!(df::DataFrame, inds)