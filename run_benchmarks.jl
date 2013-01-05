#
# Optional time-consuming Benchmarks
#

# TODO: Load DataFrames just once for all benchmarks

benchmarks = ["benchmarks/datavector.jl",
              "benchmarks/datamatrix.jl",
              "benchmarks/io.jl",
              "benchmarks/datastreams.jl"]

println("Running benchmarks:")

for benchmark in benchmarks
    println(" * $(benchmark)")
    run(`julia $(benchmark)`)
end
