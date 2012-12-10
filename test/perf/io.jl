load("DataFrames")
using DataFrames

#
# Simple Data Sets
#

simple_filename = file_path(julia_pkgdir(),"DataFrames/test/data/simple_data.csv")

N = 100

total_time = @elapsed for iter in 1:N
    df = read_table(simple_filename)
end

println(join({"Reading Simple Data Sets", N, total_time / N}, "\t"))

#
# Bigger Data Sets
#

big_filename = file_path(julia_pkgdir(),"DataFrames/test/data/big_data.csv")

N = 10

total_time = @elapsed for iter in 1:N
    df = read_table(big_filename)
end

println(join({"Reading Big Data Sets", N, total_time / N}, "\t"))
