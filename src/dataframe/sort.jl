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

    # Check that columns that shares the same underlying array are only permuted once PR#1072
    uc = [!any(j->c[i] === c[j], 1:i-1) for i=1:length(c)]

    for col in c[uc]
        copy!(pp,p)
        Base.permute!!(col, pp)
    end
    df
end
