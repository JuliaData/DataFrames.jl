using DataFrames
using CategoricalArrays
using PooledArrays
using BenchmarkTools
using Random

Random.seed!(1)

grouping_benchmarks = BenchmarkGroup()

# `refpool`/`refarray` optimized grouping method
refpool_benchmarks = grouping_benchmarks["refpool"] = BenchmarkGroup()

for k in (10, 10_000), n in (100, 100_000, 10_000_000)
    for x in (PooledArray(rand(1:k, n)),
              CategoricalArray(rand(1:n, 10_000_000)),
              PooledArray(rand([missing; 1:n], 10_000_000)),
              CategoricalArray(rand([missing; 1:n], 10_000_000)))
        df = DataFrame(x=x)

        refpool_benchmarks[k, n, nameof(typeof(x)), "skipmissing=false"] =
            @benchmarkable groupby($df, :x)

        # Skipping missing values
        refpool_benchmarks[k, n, nameof(typeof(x)), "skipmissing=true"] =
            @benchmarkable groupby($df, :x, skipmissing=true)

        # Empty group which requires adjusting group indices
        replace!(df.x, 5 => 6)
        refpool_benchmarks[k, n, nameof(typeof(x)), "empty group"] =
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