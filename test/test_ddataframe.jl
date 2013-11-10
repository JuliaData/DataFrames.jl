using DataFrames
using DataArrays

const datafile = "data/distributed/test.csv"
const nloops = 10

function load_pkgs()
    println("loading packages...")
    @everywhere using Blocks
    @everywhere using DataFrames
end

if nprocs() < 4
    addwrkrs = 4 - nprocs()
    println("adding $addwrkrs more processors...")
    addprocs(addwrkrs)
end
println("\tnprocs: $(nprocs())")
load_pkgs()

df = dreadtable(datafile, header=false)
colnames(df)
colnames!(df, ["c1","c2","c3","c4","c5","c6","c7","c8","c9","c10"])

sum_result = df+df
mul_result = 2*df
eq_result = (sum_result .== mul_result)
@assert all(eq_result)

df1 = dreadtable(open(datafile), 1000)
@assert nrow(df1) == nrow(df)
@assert ncol(df1) == ncol(df)
@assert isapprox(sum(matrix(colsums(df1))), sum(matrix(colsums(df))))
