load("DataFrames")
using DataFrames

@assert isequal(DataVec[1, 2, NA], DataVec(PooledDataVec[1, 2, NA]))
