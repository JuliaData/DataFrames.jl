i = Index()
push!(i, "A")
push!(i, "B")

inds = {1,
		1.0,
		"A",
		:A,
		[true],
		trues(1),
		[1],
		[1.0],
		1:1,
		1.0:1.0,
		["A"],
		[:A],
		DataVector[true],
		DataVector[1],
		DataVector[1.0],
		DataVector["A"],
		DataVector[:A],
		PooledDataVector[true],
		PooledDataVector[1],
		PooledDataVector[1.0],
		PooledDataVector["A"],
		PooledDataVector[:A]}

for ind in inds
	if isequal(ind, "A") || isequal(ind, :A) || ndims(ind) == 0
		@assert isequal(i[ind], 1)
	else
		@assert isequal(i[ind], [1])
	end
end

r1 = rename(i, "A", "C")
@assert isequal(r1.names, ["C", "B"])

r2 = rename(i, ["A","B"], ["C","D"])
@assert isequal(r2.names, ["C", "D"])

r3 = rename(i, {"A"=>"C"})
@assert isequal(r3.names, ["C", "B"])

r4 = rename(i, lowercase)
@assert isequal(r4.names, ["a", "b"])

@assert pop!(i) == "B"
@assert isequal(i.names, ["A"])
unshift!(i, "B")
@assert isequal(i.names, ["B", "A"])
@assert shift!(i) == "B"
@assert isequal(i.names, ["A"])


append!(i, ["B","C","D"])
@assert isequal(i.names, ["A","B","C","D"])
for (idx,s) in enumerate(["A","B","C","D"])
    @assert i.lookup[s] == idx
end

prepend!(i, ["X","Y"])
@assert isequal(i.names, ["X","Y","A","B","C","D"])
for (idx,s) in enumerate(["X","Y","A","B","C","D"])
    @assert i.lookup[s] == idx
end
