
p = PooledDataArray(DataVector[9,9,8,NA,1,1])
@assert levels(p) == [1,8,9]
@assert levels(levels!(copy(p), ["a", "b", "c"])) == ["a", "b", "c"]
@assert removeNA(levels!(copy(p), DataVector["a", "b", NA])) == ["b","a", "a"]
@assert levels(PooledDataArray(copy(p), [9,8,1])) == [9,8,1]
@assert levels(PooledDataArray(copy(p), [9,8])) == [9,8]
@assert removeNA(PooledDataArray(copy(p), [9,8])) == [9,9,8]
@assert levels(PooledDataArray(copy(p), levels(p)[[3,2,1]])) == [9,8,1]
v = [1:6]
@assert levels(reorder!(copy(p), v)) == [9,8,1]
df = @DataFrame(v => v, x => rand(6))
@assert levels(reorder!(min, copy(p), df)) == [9,8,1] 
