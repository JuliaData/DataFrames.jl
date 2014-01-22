module TestConstructors
	using Base.Test
	using DataArrays
	using DataFrames

	#
	# DataFrame
	#

	df = DataFrame()
	@test isequal(df.columns, {})
	@test isequal(df.colindex, Index())

	df = DataFrame({data(zeros(3)), data(ones(3))},
		            Index(["x1", "x2"]))
	@test size(df, 1) == 3
	@test size(df, 2) == 2

	@test isequal(df,
		          DataFrame({data(zeros(3)), data(ones(3))}))
	@test isequal(df,
		          DataFrame(x1 = [0.0, 0.0, 0.0],
		          	        x2 = [1.0, 1.0, 1.0]))

	@test isequal(df,
		          DataFrame([0.0 1.0;
		          	         0.0 1.0;
		          	         0.0 1.0],
		          ["x1", "x2"]))
	@test isequal(df,
		          DataFrame([0.0 1.0;
		          	         0.0 1.0;
		          	         0.0 1.0]))
	@test isequal(df,
		          DataFrame(data(zeros(3)), data(ones(3))))

	@test isequal(df, DataFrame({"x1" => [0.0, 0.0, 0.0],
		                         "x2" => [1.0, 1.0, 1.0]}))
	@test isequal(df, DataFrame({"x1" => [0.0, 0.0, 0.0],
		                         "x2" => [1.0, 1.0, 1.0],
		                         "x3" => [2.0, 2.0, 2.0]},
		                        ["x1", "x2"]))

	df = DataFrame(Int, 2, 2)
	@test size(df) == (2, 2)
	@test all(types(df) .== [Int, Int])

	df = DataFrame(2, 2)
	@test size(df) == (2, 2)
	@test all(types(df) .== [Float64, Float64])

	df = DataFrame([Int, Float64], ["x1", "x2"], 2)
	@test size(df) == (2, 2)
	@test all(types(df) .== {Int, Float64})

	@test isequal(df, DataFrame([Int, Float64], 2))
end
