using DataFrames
using PooledArrays
using BenchmarkTools
using Random

Random.seed!(1)

grouping_benchmarks = BenchmarkGroup()

for n in (1000, 10_000_000),
    k in (10, 10_000),
    x in (rand(1:k, n),
          rand([1:k; missing], n),
          rand(string.(1:k), n),
          rand([string.(1:k); missing], n),
          PooledArray(rand(1:k, n)),
          PooledArray(rand([missing; 1:k], n)))
        df = DataFrame(x=x)

        grouping_benchmarks[n, k, typeof(x), "skipmissing=false"] =
            @benchmarkable groupby($df, :x)

        # Skipping missing values
        grouping_benchmarks[n, k, typeof(x), "skipmissing=true"] =
            @benchmarkable groupby($df, :x, skipmissing=true)

        for l in (10, 10_000),
            y in (rand(1:l, n),
                  rand([1:l; missing], n),
                  rand(string.(1:l), n),
                  rand([string.(1:l); missing], n),
                  PooledArray(rand(1:l, n)),
                  PooledArray(rand([missing; 1:l], n)))
            df.y = y

            grouping_benchmarks[n, (k, l), (typeof(x), typeof(y)),
                                "skipmissing=false"] =
                @benchmarkable groupby($df, [:x, :y])

            # Skipping missing values
            grouping_benchmarks[n, (k, l), (typeof(x), typeof(y)),
                                "skipmissing=true"] =
                @benchmarkable groupby($df, [:x, :y], skipmissing=true)
        end

        if df.x isa PooledArray
            # Empty group which requires adjusting group indices
            replace!(df.x, levels(df.x)[1] => levels(df.x)[2])
            grouping_benchmarks[n, k, typeof(x), "empty group"] =
                @benchmarkable groupby($df, :x)
        end
end


# If a cache of tuned parameters already exists, use it, otherwise, tune and cache
# the benchmark parameters. Reusing cached parameters is faster and more reliable
# than re-tuning `suite` every time the file is included.
paramspath = joinpath(dirname(@__FILE__), "params.json")

if isfile(paramspath)
    loadparams!(grouping_benchmarks, BenchmarkTools.load(paramspath)[1], :evals);
else
    tune!(grouping_benchmarks)
    BenchmarkTools.save(paramspath, params(grouping_benchmarks));
end

grouping_results = run(grouping_benchmarks, verbose=true)
# using Serialization
# serialize("grouping_results.jls", grouping_results)
# leaves(judge(median(grouping_results1), median(grouping_results2)))
# leaves(regressions(judge(median(grouping_results1), median(grouping_results2))))
# leaves(improvements(judge(median(grouping_results1), median(grouping_results2))))