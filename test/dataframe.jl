require("extras/test.jl")

load("DataFrames")
using DataFrames

test_group("Operations on DataFrames that have column groupings")

x = DataFrame(quote
  a = [1,2,3]
  b = [4,5,6]
end)
y = DataFrame(quote
  c = [1,2,3]
  d = [4,5,6]
end)

set_group(x, "group1", ["a", "b"])
set_group(y, "group2", ["c", "d"])

z = deepcopy(x)  
@test is_group(z, "group1")

z = cbind(x, y)
@test is_group(z, "group1")
@test is_group(z, "group2")

v = DataFrame(quote
  a = [5,6,7]
  b = [8,9,10]
end)
z = rbind({v, x})
@test is_group(z, "group1")

z = rbind(v,x)
@test is_group(z, "group1")

# Deleting columns removes any mention from groupings
del!(x, "a")
@test colnames(x) == ["b"]
@test get_groups(x)["group1"] == ["b"]

## del calls ref, which properly deals with groupings
y = del(y, "c")
@test colnames(y) == ["d"]
@test get_groups(y)["group2"] == ["d"]
z1 = z[[1]]
@test colnames(z1) == ["a"]
@test get_groups(z1)["group1"] == ["a"]

z2 = z[:,[1,1,2]]
@test colnames(z2) == ["a", "a_1", "b"]
@test get_groups(z2)["group1"] == ["a_1", "b"]

test_group("DataFrame assignment")
df1 = DataFrame(quote
    a = 1:5
    b2 = letters[1:5]
    v2 = randn(5) 
end)
df2 = DataFrame(quote
    a = reverse([1:5])
    b2 = reverse(letters)[1:5]
    v2 = randn(5)
end)
df1[1:2,:] = df2[4:5,:]
@test df1[1:2,:] == df2[4:5,:]

test_group("Empty DataFrame constructors")
df = DataFrame(10, 5)
@assert nrow(df) == 10
@assert ncol(df) == 5
@assert typeof(df[:, 1]) == DataVec{Float64}

df = DataFrame(Int64, 10, 3)
@assert nrow(df) == 10
@assert ncol(df) == 3
@assert typeof(df[:, 1]) == DataVec{Int64}
@assert typeof(df[:, 2]) == DataVec{Int64}
@assert typeof(df[:, 3]) == DataVec{Int64}

df = DataFrame({Int64, Float64, ASCIIString}, 100)
@assert nrow(df) == 100
@assert ncol(df) == 3
@assert typeof(df[:, 1]) == DataVec{Int64}
@assert typeof(df[:, 2]) == DataVec{Float64}
@assert typeof(df[:, 3]) == DataVec{ASCIIString}

df = DataFrame({Int64, Float64, ASCIIString}, ["A", "B", "C"], 100)
@assert nrow(df) == 100
@assert ncol(df) == 3
@assert typeof(df[:, 1]) == DataVec{Int64}
@assert typeof(df[:, 2]) == DataVec{Float64}
@assert typeof(df[:, 3]) == DataVec{ASCIIString}

df = DataFrame(zeros(10, 5))
@assert nrow(df) == 10
@assert ncol(df) == 5
@assert typeof(df[:, 1]) == DataVec{Float64}

df = DataFrame(ones(10, 5))
@assert nrow(df) == 10
@assert ncol(df) == 5
@assert typeof(df[:, 1]) == DataVec{Float64}

df = DataFrame(eye(10, 5))
@assert nrow(df) == 10
@assert ncol(df) == 5
@assert typeof(df[:, 1]) == DataVec{Float64}

test_group("Other DataFrame constructors")
df = DataFrame([{"a"=>1, "b"=>'c'}, {"a"=>3, "b"=>'d'}, {"a"=>5}])
@assert nrow(df) == 3
@assert ncol(df) == 2
@assert typeof(df[:,"a"]) == DataVec{Int}
@assert typeof(df[:,"b"]) == DataVec{Char}

df = DataFrame([{"a"=>1, "b"=>'c'}, {"a"=>3, "b"=>'d'}, {"a"=>5}], ["a", "b"])
@assert nrow(df) == 3
@assert ncol(df) == 2
@assert typeof(df[:,"a"]) == DataVec{Int}
@assert typeof(df[:,"b"]) == DataVec{Char}

data = {"A" => [1, 2], "C" => ["1", "2"], "B" => [3, 4]}
df = DataFrame(data)
# Specify column_names
df = DataFrame(data, ["C", "A", "B"])

# This assignment was missing before
df = DataFrame(quote Column = ["A"] end)
df[1, "Column"] = "Testing"
