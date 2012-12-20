autocor([1, 2, 3, 4, 5])
autocor(DataVec([1, 2, 3, 4, 5]))

@assert iqr([1, 2, 3, 4, 5]) == [2.0, 4.0]

z = [true, true, false, false, true, false, true, true, true]
values, lengths = rle(z)
@assert values == [true, false, true, false, true]
@assert lengths == [2, 2, 1, 1, 3]
@assert inverse_rle(values, lengths) == z

z = [true, true, false, false, true, false, true, true, true, false]
values, lengths = rle(z)
@assert values == [true, false, true, false, true, false]
@assert lengths == [2, 2, 1, 1, 3, 1]
@assert inverse_rle(values, lengths) == z

@assert norm(dist([1 0; 0 1]) - [0.0 sqrt(2); sqrt(2) 0.0]) < 10e-8
@assert norm(dist([3.0 1.0; 5.0 1.0]) - [0.0 2.0; 2.0 0.0]) < 10e-8
@assert norm(dist([1 0 0; 0 1 0 ; 1 0 1]) - [0.0 sqrt(2) 1.0; sqrt(2) 0.0 sqrt(3); 1.0 sqrt(3) 0.0]) < 10e-8
