#
# Optional time-consuming Benchmarks
#

using DataFrames
using Benchmark

benchmarks = ["benchmarks/datavector.jl",
              "benchmarks/datamatrix.jl",
              "benchmarks/io.jl"]
              # TODO: Restores DataStream
              #"benchmarks/datastreams.jl"]

# TODO: Print summary to stdout_stream, while printing results
#       to file with appends.
#println("Running benchmarks:")

for benchmark in benchmarks
#    println(" * $(benchmark)")
    include(benchmark)
end
