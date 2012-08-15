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

