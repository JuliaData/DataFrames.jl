import PrecompileTools

PrecompileTools.@compile_workload begin
    for (S, I) in ((String, Int32), (InlineStrings.String3, Int64),
                   (InlineStrings.String7, Int32), (InlineStrings.String15, Int64))
        df = DataFrame(a=I[2, 5, 3, 1, 0], b=S["a", "b", "c", "a", "b"], c=collect(I, 1:5),
                       p=PooledArray(S["a", "b", "c", "a", "b"]),
                       q=[true, false, true, false, true],
                       f=Float64[2, 5, 3, 1, 0])
        describe(df)
        names(df[1, 1:2])
        sort(df, :a)
        combine(df, :c, [:c :f] .=> [sum, mean, std], :c => :d, [:a, :c] => cor)
        transform(df, :c, [:c :f] .=> [sum, mean, std], :c => :d, [:a, :c] => cor)
        foreach(col -> groupby(df, col), (:a, :p, :q))
        gdf = groupby(df, :b)
        combine(gdf, :c, [:c :f] .=> [sum∘skipmissing, mean∘skipmissing, std∘skipmissing],
                :c => :d, [:a, :c] => cor)
        transform(gdf, :c, [:c :f] .=> [sum∘skipmissing, mean∘skipmissing, std∘skipmissing],
                  :c => :d, [:a, :c] => cor)
        for oncol in (:a, :b, :c, :p)
            innerjoin(df, df, on=oncol, makeunique=true)
            leftjoin(df, df, on=oncol, makeunique=true)
            outerjoin(df, df, on=oncol, makeunique=true)
        end
        leftjoin!(df, DataFrame(a=[2, 5, 3, 1, 0]), on=:a)
        leftjoin!(df, DataFrame(b=["a", "b", "c", "d", "e"]), on=:b)
        leftjoin!(df, DataFrame(c=1:5), on=:c)
        reduce(vcat, [df, df])
        show(IOBuffer(), df)
        subset(df, :q)
        subset!(copy(df), :q)
        df[:, 1:2]
        df[1:2, :]
        df[1:2, 1:2]
        @view df[:, 1:2]
        @view df[1:2, :]
        @view df[1:2, 1:2]
        transform!(df, :c, [:c :f] .=> [sum, mean, std], :c => :d, [:a, :c] => cor)
        deleteat!(df, 1)
        append!(df, copy(df))
        push!(df, copy(df[1, :]))
        eachrow(df)
        eachcol(df)
        empty(df)
        empty!(copy(df))
        filter(:q => identity, df)
        filter!(:q => identity, df)
        first(df)
        last(df)
        hcat(df, df, makeunique=true)
        issorted(df)
        pop!(df)
        popfirst!(df)
        repeat(df, 2)
        reverse(df)
        reverse!(df)
        unique(df, :a)
        unique!(df, :a)
        wide = DataFrame(id=1:6,
                         a=repeat(1:3, inner=2),
                         b=repeat(1.0:2.0, inner=3),
                         c=repeat(1.0:1.0, inner=6),
                         d=repeat(1.0:3.0, inner=2))
        long = stack(wide)
        unstack(long)
        unstack(long, :variable, :value, combine=sum)
        flatten(DataFrame(a=[[1, 2], [3, 4]], b=[1, 2]), :a)
        dropmissing(DataFrame(a=[1, 2, 3, missing], b=["a", missing, "c", "d"]))
        df = DataFrame(rand(20, 2), :auto)
        df.id = repeat(1:2, 10)
        combine(df, AsTable(r"x") .=> [ByRow(sum), ByRow(mean)])
        combine(groupby(df, :id), AsTable(r"x") .=> [ByRow(sum), ByRow(mean)])
    end
end
