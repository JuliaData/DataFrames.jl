module TestConversions
	using Base.Test
	using DataArrays
	using DataFrames

	df = DataFrame()
	df["A"] = 1:5
	df["B"] = ["A", "B", "C", "D", "E"]
	@assert isa(matrix(df), Matrix{Any})
	@assert isa(matrix(df, Any), Matrix{Any})

	df = DataFrame()
	df["A"] = 1:5
	df["B"] = 1.0:5.0
	@assert isa(matrix(df), Matrix{Real})
	@assert isa(matrix(df, Any), Matrix{Any})
	@assert isa(matrix(df, Float64), Matrix{Float64})

	df = DataFrame()
	df["A"] = 1.0:5.0
	df["B"] = 1.0:5.0
	@assert isa(matrix(df), Matrix{Float64})
	@assert isa(matrix(df, Any), Matrix{Any})
	@assert isa(matrix(df, Int), Matrix{Int})
end
