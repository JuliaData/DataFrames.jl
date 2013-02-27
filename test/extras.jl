##########
## table
##########


d = DataVector[1,1,2,4,5,9,NA,NA]
t = table(d)
@assert t[1] == 2
@assert t[NA] == 2

x = LETTERS[[1,1,9,5,9,9,9]]
t = table(x)
@assert t["A"] == 2
@assert t["I"] == 4
@assert length(t) == 3

##########
## paste
##########

@assert paste(["a", "b"], "X", [1:2]) == ["aX1", "bX2"]
@assert paste(["a", "b"], "X", [1:4]) == ["aX1", "bX2", "aX3", "bX4"]

##########
## cut
##########

@assert isequal(cut([2, 3, 5], [1, 3, 6]), PooledDataArray(["(1,3]", "(1,3]", "(3,6]"]))
@assert isequal(cut([2, 3, 5], [3, 6]), PooledDataArray(["[2,3]", "[2,3]", "(3,6]"]))
@assert isequal(cut([2, 3, 5, 6], [3, 6]), PooledDataArray(["[2,3]", "[2,3]", "(3,6]", "(3,6]"]))
@assert isequal(cut([1, 2, 4], [1, 3, 6]), PooledDataArray(["[1,3]", "[1,3]", "(3,6]"]))
@assert isequal(cut([1, 2, 4], [3, 6]), PooledDataArray(["[1,3]", "[1,3]", "(3,6]"]))
@assert isequal(cut([1, 2, 4], [3]), PooledDataArray(["[1,3]", "[1,3]", "(3,4]"]))
@assert isequal(cut([1, 5, 7], [3, 6]), PooledDataArray(["[1,3]", "(3,6]", "(6,7]"]))

ages = [20, 22, 25, 27, 21, 23, 37, 31, 61, 45, 41, 32]
bins = [18, 25, 35, 60, 100]
cats = cut(ages, bins)
pdv = PooledDataArray(["(18,25]", "(18,25]", "(18,25]",
                       "(25,35]", "(18,25]", "(18,25]",
                       "(35,60]", "(25,35]", "(60,100]",
                       "(35,60]", "(35,60]", "(25,35]"])
@assert isequal(cats, pdv)


##########
## rep
##########

@assert rep(3, 2) == [3,3]
@assert rep([3,4], 2) == [3,4,3,4]
@assert rep([3,4], [2,3]) == [3,3,4,4,4]
@assert isequal(rep(DataVector[NA,3,4], 2), DataVector[NA,3,4,NA,3,4])
@assert isequal(rep(DataVector[NA,3,4], [2,1,2]), DataVector[NA,NA,3,4,4])
@assert isequal(rep(DataVector[NA,3,4], [2,1,0]), DataVector[NA,NA,3])


##########
## findat
##########

@assert findat([4,1,9,10,1], [1:6]) == [4,1,0,0,1]
@assert findat([1:6], [1,5,9,3,1]) == [1,0,4,0,2,0]
