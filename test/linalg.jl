load("DataFrames")
using DataFrames

d = dmeye(3, 3)
d[1, 1] = NA

svd(d)
