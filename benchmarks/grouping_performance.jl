using DataFrames
using CategoricalArrays
using PooledArrays
using BenchmarkTools


# `refpool`/`refarray` optimized grouping method
for k in (10, 10_000), n in (100, 100_000, 10_000_000)
    for x in (PooledArray(rand(1:k, n)),
              CategoricalArray(rand(1:n, 10_000_000)),
              PooledArray(rand([missing; 1:n], 10_000_000)),
              CategoricalArray(rand([missing; 1:n], 10_000_000)))
        df = DataFrame(x=x)
        @btime groupby($df, :x)

        # Skipping missing values
        @btime groupby($df, :x, skipmissing=true)

        # Empty group which requires adjusting group indices
        replace!(df.x, 5 => 6)
        @btime groupby($df, :x)
    end
end