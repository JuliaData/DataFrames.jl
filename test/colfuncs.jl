using Base.Test
using DataFrames

let
	m = [1 2 3; 3 4 6]
	df = DataFrame(m)
	@assert isequal(DataFrame([2.0 3.0 4.5;], colnames(df)), colmeans(df))
	@assert isequal(DataFrame([4 6 9;], colnames(df)), colsums(df))
end
