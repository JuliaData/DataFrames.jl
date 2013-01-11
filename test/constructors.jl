#
# NA's
#

@assert isna(NAtype())
@assert isna(NA)

#
# DataVector's
#

dv = DataArray([1, 2, 3], falses(3))
@assert isequal(dv.data, [1, 2, 3])
@assert isequal(dv.na, falses(3))
@assert isequal(dv, DataArray([1, 2, 3], [false, false, false]))
@assert isequal(dv, DataArray([1, 2, 3]))

dv = DataArray(trues(3), falses(3))
@assert isequal(dv.data, [true, true, true])
@assert isequal(dv.na, falses(3))
@assert isequal(dv, DataArray(trues(3)))

dv = DataArray([1, 2, 3], falses(3))
@assert isequal(dv, DataArray(1:3))
@assert isequal(dv, DataArray(DataArray([1, 2, 3])))

dv = DataArray(Int64, 3)
@assert isequal(eltype(dv), Int64)
@assert isequal(dv.na, trues(3))

# dv = DataArray(3)
# @assert isequal(eltype(dv), Float64)
# @assert isequal(dv.na, trues(3))

# dv = DataArray()
# @assert isequal(eltype(dv), Float64)
# @assert isequal(dv.na, trues(0))

@assert isequal(datazeros(3), DataArray(zeros(3)))
@assert isequal(datazeros(Int64, 3), DataArray(zeros(Int64, 3)))
@assert isequal(dataones(3), DataArray(ones(3)))
@assert isequal(dataones(Int64, 3), DataArray(ones(Int64, 3)))
@assert isequal(datafalses(3), DataArray(falses(3)))
@assert isequal(datatrues(3), DataArray(trues(3)))

dv = DataVector[1, 2, NA]
@assert dv[1] == 1
@assert dv[2] == 2
@assert isna(dv[3])
@assert isequal(eltype(dv), Int64)

#
# PooledDataArray's
#

pdv = PooledDataArray([1, 2, 3], falses(3))
@assert all(pdv .== [1, 2, 3])
@assert all(isna(pdv) .== falses(3))

@assert isequal(pdv, PooledDataArray([1, 2, 3], [false, false, false]))
@assert isequal(pdv, PooledDataArray([1, 2, 3]))

pdv = PooledDataArray(trues(3), falses(3))
@assert all(pdv .== [true, true, true])
@assert all(isna(pdv) .== falses(3))
@assert isequal(pdv, PooledDataArray(trues(3)))

pdv = PooledDataArray([1, 2, 3], falses(3))
@assert isequal(pdv, PooledDataArray(1:3))
@assert isequal(pdv, PooledDataArray(PooledDataArray([1, 2, 3])))

pdv = PooledDataArray(Int64, 3)
@assert isequal(eltype(pdv), Int64)
@assert all(isna(pdv) .== trues(3))

# pdv = PooledDataArray(3)
# @assert isequal(eltype(pdv), Float64)
# @assert all(isna(pdv) .== trues(3))

# pdv = PooledDataArray()
# @assert isequal(eltype(pdv), Float64)
# @assert all(isna(pdv) .== trues(0))

@assert isequal(pdatazeros(3), PooledDataArray(zeros(3)))
@assert isequal(pdatazeros(Int64, 3), PooledDataArray(zeros(Int64, 3)))
@assert isequal(pdataones(3), PooledDataArray(ones(3)))
@assert isequal(pdataones(Int64, 3), PooledDataArray(ones(Int64, 3)))
@assert isequal(pdatafalses(3), PooledDataArray(falses(3)))
@assert isequal(pdatatrues(3), PooledDataArray(trues(3)))

pdv = PooledDataVector[1, 2, NA]
@assert pdv[1] == 1
@assert pdv[2] == 2
@assert isna(pdv[3])
@assert isequal(eltype(pdv), Int64)

#
# DataMatrix
#

dm = DataArray([1 2; 3 4], falses(2, 2))
@assert isequal(dm.data, [1 2; 3 4])
@assert isequal(dm.na, falses(2, 2))

@assert isequal(dm, DataArray([1 2; 3 4], [false false; false false]))
@assert isequal(dm, DataArray([1 2; 3 4]))

dm = DataArray(trues(2, 2), falses(2, 2))
@assert isequal(dm.data, trues(2, 2))
@assert isequal(dm.na, falses(2, 2))

@assert isequal(dm, DataArray(trues(2, 2)))

#DataMatrix(dvzeros(3), dvzeros(3))
#DataMatrix(1:3, 1:3)

@assert isequal(DataArray([1 2; 3 4]), DataArray(DataArray([1 2; 3 4])))

dm = DataArray(Int64, 2, 2)
@assert isequal(eltype(dm), Int64)
@assert isequal(dm.na, trues(2, 2))

# dm = DataArray(2, 2)
# @assert isequal(eltype(dm), Float64)
# @assert isequal(dm.na, trues(2, 2))

# dm = DataArray(Int64)
# @assert isequal(eltype(dm), Int64)
# @assert isequal(dm.na, trues(0, 0))

# dm = DataArray()
# @assert isequal(eltype(dm), Float64)
# @assert isequal(dm.na, trues(0, 0))

@assert isequal(datazeros(2, 2), DataArray(zeros(2, 2)))
@assert isequal(datazeros(Int64, 2, 2), DataArray(zeros(Int64, 2, 2)))

@assert isequal(dataones(2, 2), DataArray(ones(2, 2)))
@assert isequal(dataones(Int64, 2, 2), DataArray(ones(Int64, 2, 2)))

@assert isequal(datafalses(2, 2), DataArray(falses(2, 2)))
@assert isequal(datatrues(2, 2), DataArray(trues(2, 2)))

@assert isequal(dataeye(3, 2), DataArray(eye(3, 2)))
@assert isequal(dataeye(2), DataArray(eye(2)))
@assert isequal(datadiagm([pi, pi]), DataArray(diagm([pi, pi])))

#
# DataFrame
#

df = DataFrame()
@assert isequal(df.columns, {})
# TODO: Get this to work
#@assert isequal(df.colindex, Index())

df = DataFrame({datazeros(3), dataones(3)}, Index(["x1", "x2"]))
@assert nrow(df) == 3
@assert ncol(df) == 2

# TODO: Make isequal fail if colnames don't match
@assert isequal(df, DataFrame({datazeros(3), dataones(3)}))
@assert isequal(df, DataFrame(quote x1 = [0.0, 0.0, 0.0]; x2 = [1.0, 1.0, 1.0] end))

@assert isequal(df, DataFrame([0.0 1.0; 0.0 1.0; 0.0 1.0], ["x1", "x2"]))
@assert isequal(df, DataFrame([0.0 1.0; 0.0 1.0; 0.0 1.0]))
@assert isequal(df, DataFrame(datazeros(3), dataones(3)))

# TODO: Fill these in
# From (Associative): ???
# From (Vector, Vector, Groupings): ???

@assert isequal(df, DataFrame({"x1" => [0.0, 0.0, 0.0], "x2" => [1.0, 1.0, 1.0]}))
@assert isequal(df, DataFrame({"x1" => [0.0, 0.0, 0.0], "x2" => [1.0, 1.0, 1.0], "x3" => [2.0, 2.0, 2.0]}, ["x1", "x2"]))

df = DataFrame(Int64, 2, 2)
@assert size(df) == (2, 2)
@assert all(coltypes(df) .== {Int64, Int64})
@assert all(isna(df))

df = DataFrame(2, 2)
@assert size(df) == (2, 2)
@assert all(coltypes(df) .== {Float64, Float64})
@assert all(isna(df))

df = DataFrame({Int64, Float64}, ["x1", "x2"], 2)
@assert size(df) == (2, 2)
@assert all(coltypes(df) .== {Int64, Float64})
@assert all(isna(df))

@assert isequal(df, DataFrame({Int64, Float64}, 2))
