#
# Optional time-consuming Benchmarks
#

using DataFrames
using Benchmark

benchmarks = [ "io.jl"]

# TODO: Print summary to stdout_stream, while printing results
#       to file with appends.
#println("Running benchmarks:")

for benchmark in benchmarks
#    println(" * $(benchmark)")
    include(benchmark)
end
