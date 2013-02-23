
p = PooledDataArray(DataVector[9,9,8,NA,1,1])
pcopy = copy(p)
@assert levels(p) == [1,8,9]
@assert levels(set_levels(p, ["a", "b", "c"])) == ["a", "b", "c"]
@assert removeNA(set_levels(p, DataVector["a", "b", NA])) == ["b", "a", "a"]
@assert removeNA(set_levels(p, DataVector["a", "b", "a"])) == ["a", "a", "b", "a", "a"]
@assert levels(set_levels(p, DataVector["a", "b", "a"])) == ["a", "b"]
@assert levels(set_levels(p, [1 => 111])) == [111, 8, 9]
@assert levels(set_levels(p, [1 => 111, 8 => NA])) == [111, 9]
@assert levels(PooledDataArray(p, [9,8,1])) == [9,8,1]
@assert levels(PooledDataArray(p, [9,8])) == [9,8]
@assert removeNA(PooledDataArray(p, [9,8])) == [9,9,8]
@assert levels(PooledDataArray(p, levels(p)[[3,2,1]])) == [9,8,1]
v = [1:6]
@assert isequal(p, reorder(p))
@assert levels(reorder(p, v)) == [9,8,1]
df = @DataFrame(v => v, x => rand(6))
@assert levels(reorder(min, p, df)) == [9,8,1] 
@assert isequal(p, pcopy)

@assert levels(set_levels!(copy(p), [10,80,90])) == [10, 80, 90]
@assert levels(set_levels!(copy(p), [1,8,1])) == [1, 8]
@assert levels(set_levels!(copy(p), DataVector[1,8,NA])) == [1, 8]
@assert levels(set_levels!(copy(p), [1,8,9, 10])) == [1, 8, 9, 10]
@assert levels(set_levels!(copy(p), [1 => 111])) == [111, 8, 9]
@assert levels(set_levels!(copy(p), [1 => 111, 8 => NA])) == [111, 9]
