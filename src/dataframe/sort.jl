function Base.sort!(df::DataFrame; cols=Any[], alg=nothing,
                    lt=isless, by=identity, rev=false, order=Forward)
    if !(isa(by, Function) || eltype(by) <: Function)
        msg = "'by' must be a Function or a vector of Functions. Perhaps you wanted 'cols'."
        throw(ArgumentError(msg))
    end
    ord = ordering(df, cols, lt, by, rev, order)
    _alg = Sort.defalg(df, ord; alg=alg, cols=cols)
    sort!(df, _alg, ord)
end

function Base.sort!(df::DataFrame, a::Base.Sort.Algorithm, o::Base.Sort.Ordering)
    p = sortperm(df, a, o)
    pp = similar(p)
    c = columns(df)

    for (i,col) in enumerate(c)
        # Check if this column has been sorted already
        if any(j -> c[j]===col, 1:i-1)
            continue
        end

        copy!(pp,p)
        Base.permute!!(col, pp)
    end
    df
end
