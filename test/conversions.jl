@assert isequal(DataVector[1, 2, NA],
	            DataArray(PooledDataVector[1, 2, NA]))

# Test vector() and matrix() conversion tools
dv = dataones(5)
@assert isa(vector(dv), Vector{Float64})
@assert isa(convert(Vector{Float64}, dv), Vector{Float64})
dv[1] = NA
# Should raise errors:
# vector(dv)
# convert(Vector{Float64}, dv)
@assert isa(vector(dv, Any), Vector{Any})
@assert isnan(vector(dv, Float64, NaN)[1])

dm = dataones(3, 3)
@assert isa(matrix(dm), Matrix{Float64})
@assert isa(convert(Matrix{Float64}, dm), Matrix{Float64})
dm[1, 1] = NA
# Should raise errors:
# matrix(dm)
# convert(Matrix{Float64}, dm)
@assert isa(matrix(dm, Any), Matrix{Any})
@assert isnan(matrix(dm, Float64, NaN)[1, 1])

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
@assert isa(matrix(df, Int64), Matrix{Int64})
