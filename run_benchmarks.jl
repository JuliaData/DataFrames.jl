#
# Optional time-consuming Benchmarks
#

# TODO: Load DataFrames just once for all benchmarks

benchmarks = ["test/perf/datavec.jl",
              "test/perf/io.jl",
              "test/perf/datastreams.jl",
              "test/perf/datamatrix.jl"]

println("Running benchmarks:")

for benchmark in benchmarks
    println(" * $(benchmark)")
    run(`julia $(benchmark)`)
end
