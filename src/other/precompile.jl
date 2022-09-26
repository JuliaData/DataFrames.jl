import SnoopPrecompile

SnoopPrecompile.@precompile_all_calls @time begin
    df = DataFrame(a=[2, 5, 3, 1, 0], b=["a", "b", "c", "a", "b"], c=1:5,
                   p=PooledArray(["a", "b", "c", "a", "b"]),
                   q=[true, false, true, false, true],
                   f=Float64[2, 5, 3, 1, 0])
    describe(df)
    names(df[1, 1:2])
    sort(df, :a)
    combine(df, :c, [:c :f] .=> [sum, mean, std], :c => :d, [:a, :c] => cor)
    transform(df, :c, [:c :f] .=> [sum, mean, std], :c => :d, [:a, :c] => cor)
    groupby(df, :a)
    groupby(df, :q)
    groupby(df, :p)
    gdf = groupby(df, :b)
    combine(gdf, :c, [:c :f] .=> [sum, mean, std], :c => :d, [:a, :c] => cor)
    transform(gdf, :c, [:c :f] .=> [sum, mean, std], :c => :d, [:a, :c] => cor)
    innerjoin(df, df, on=:a, makeunique=true)
    innerjoin(df, df, on=:b, makeunique=true)
    innerjoin(df, df, on=:c, makeunique=true)
    outerjoin(df, df, on=:a, makeunique=true)
    outerjoin(df, df, on=:b, makeunique=true)
    outerjoin(df, df, on=:c, makeunique=true)
    semijoin(df, df, on=:a)
    semijoin(df, df, on=:b)
    semijoin(df, df, on=:c)
    leftjoin!(df, DataFrame(a=[2, 5, 3, 1, 0]), on=:a)
    leftjoin!(df, DataFrame(b=["a", "b", "c", "d", "e"]), on=:b)
    leftjoin!(df, DataFrame(c=1:5), on=:c)
    reduce(vcat, [df, df])
    show(IOBuffer(), df)
    subset(df, :q)
    @view df[1:3, :]
    @view df[:, 1:2]
    select!(df, :c, [:c :f] .=> [sum, mean, std], :c => :d, [:a, :c] => cor)
end
