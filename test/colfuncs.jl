m = [1 2 3; 3 4 6]
dm = DataArray(m)
df = DataFrame(m)
@assert isequal(DataFrame([2.0 3.0 4.5;], colnames(df)), colmeans(df))
@assert isequal(DataFrame([4 6 9;], colnames(df)), colsums(df))
@assert isequal(DataArray([2.0, 3.0, 4.5]), colmeans(dm))
