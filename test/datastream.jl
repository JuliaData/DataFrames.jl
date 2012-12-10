require("extras/test.jl")

load("DataFrames")
using DataFrames

# Need to deal with no header cleanly
filename = file_path(julia_pkgdir(), "DataFrames/test/data/sample_data.csv")

ds = DataStream(filename)
df = start(ds)
(new_df, new_df) = next(ds, df)
@assert done(ds, new_df) == false
(new_df, new_df) = next(ds, new_df)
@assert done(ds, new_df) == false
(new_df, new_df) = next(ds, new_df)
@assert done(ds, new_df) == false
(new_df, new_df) = next(ds, new_df)
@assert done(ds, new_df) == true

filename = file_path(julia_pkgdir(), "DataFrames/test/data/big_data.csv")

ds = DataStream(filename, 100)
df = start(ds)
(new_df, new_df) = next(ds, df)
@assert done(ds, new_df) == false

ds = DataStream(filename, 5)

for minibatch in ds
  @assert isa(minibatch, DataFrame)
end

means = colmeans(ds)
@assert abs(means[1, 4] - (-0.005686449)) < 10e-4
@assert abs(means[1, 5] - 19.01197) < 10e-4

vars = colvars(ds)
@assert abs(vars[1, 4] - 0.98048) < 10e-4
@assert abs(vars[1, 5] - 0.989416) < 10e-4

(mins, maxs) = colranges(ds)
@assert abs(mins[1, 4] - (-4.33635)) < 10e-4
@assert abs(mins[1, 5] - 15.6219) < 10e-4
@assert abs(maxs[1, 4] - 3.86857) < 10e-4
@assert abs(maxs[1, 5] - 22.574) < 10e-4

covariances = cov(ds)
for i in ncol(covariances)
  @assert abs(covariances[i, i] - vars[1, i]) < 10e-4
end
@assert abs(covariances[4, 4] - 0.980479916) < 10e-4
@assert abs(covariances[4, 5] - 0.009823644) < 10e-4
@assert abs(covariances[5, 4] - 0.009823644) < 10e-4
@assert abs(covariances[5, 5] - 0.989415811) < 10e-4

correlations = cor(ds)
for i in ncol(correlations)
  for j in ncol(correlations)
    true_value = covariances[i, j] / sqrt(covariances[i, i] * covariances[j, j])
    @assert abs(correlations[i, j] - true_value) < 10e-4
  end
end

# Deal with different delimiters
filename = file_path(julia_pkgdir(), "DataFrames/test/data/sample_data.csv")
ds = DataStream(filename)
for row in ds
  @assert nrow(row) <= 1
end

filename = file_path(julia_pkgdir(), "DataFrames/test/data/sample_data.tsv")
ds = DataStream(filename)
for row in ds
  @assert nrow(row) <= 1
end

filename = file_path(julia_pkgdir(), "DataFrames/test/data/sample_data.wsv")
ds = DataStream(filename)
for row in ds
  @assert nrow(row) <= 1
end

# DataFrame to DataStream conversion
df = DataFrame(quote A = 1:25 end)

ds = DataStream(df, 5)

for mini in ds
	@assert nrow(mini) <= 5
end

colmeans(ds)
