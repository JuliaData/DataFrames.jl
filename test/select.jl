module TestSelect
	using DataArrays
	using DataFrames

	df = DataFrame(A = 1:3, B = [2, 1, 2])
	x = [2, 1, 0]

	@select df :A .> 1
	@select df :B .> 1
	@select df :A .> x
	@select df :B .> x
	@select df :A .> :B
	@select df sin(:A) .> cos(:B)
	@select df cos(:A) .> sin(:B)
	@select df cos(x) .> sin(:B) + 1
	@select df sin(x) .> sin(:B) + 1
	@select df sin(:A) .> sin(:B) + 1
	@select df sin(:A) .> sin(:B)
	@select df sin(:A) .> exp(cos(:B) - 1)
end
