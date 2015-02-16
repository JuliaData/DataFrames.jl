function Base.sort!(df::DataFrame; cols=Any[], alg=nothing,
                    lt=isless, by=identity, rev=false, order=Forward)
    if !(isa(by, Function) || isa(by, Vector{Function}))
        msg = "'by' must be a Function or AbstractVector{Function}. Perhaps you wanted 'cols'."
        throw(ArgumentError(msg))
    end
    ord = ordering(df, cols, lt, by, rev, order)
    _alg = Sort.defalg(df, ord; alg=alg, cols=cols)
    sort!(df, _alg, ord)
end

function Base.sort!(df::DataFrame, a::Base.Sort.Algorithm, o::Base.Sort.Ordering)
    p = sortperm(df, a, o)
    pp = similar(p)
    for col in columns(df)
        copy!(pp,p)
        Base.permute!!(col, pp)
    end
    df
end
