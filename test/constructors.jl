load("DataFrames")
using DataFrames

#
# NA's
#

@assert isna(NAtype())
@assert isna(NA)

#
# DataVec's
#

dv = DataVec([1, 2, 3], falses(3))
@assert isequal(dv.data, [1, 2, 3])
@assert isequal(dv.na, falses(3))
@assert isequal(dv, DataVec([1, 2, 3], [false, false, false]))
@assert isequal(dv, DataVec([1, 2, 3]))

dv = DataVec(trues(3), falses(3))
@assert isequal(dv.data, [true, true, true])
@assert isequal(dv.na, falses(3))
@assert isequal(dv, DataVec(trues(3)))

dv = DataVec([1, 2, 3], falses(3))
@assert isequal(dv, DataVec(1:3))
@assert isequal(dv, DataVec(DataVec([1, 2, 3])))

dv = DataVec(Int64, 3)
@assert isequal(eltype(dv), Int64)
@assert isequal(dv.na, trues(3))

dv = DataVec(3)
@assert isequal(eltype(dv), Float64)
@assert isequal(dv.na, trues(3))

dv = DataVec()
@assert isequal(eltype(dv), Float64)
@assert isequal(dv.na, trues(0))

@assert isequal(dvzeros(3), DataVec(zeros(3)))
@assert isequal(dvzeros(Int64, 3), DataVec(zeros(Int64, 3)))
@assert isequal(dvones(3), DataVec(ones(3)))
@assert isequal(dvones(Int64, 3), DataVec(ones(Int64, 3)))
@assert isequal(dvfalses(3), DataVec(falses(3)))
@assert isequal(dvtrues(3), DataVec(trues(3)))

dv = DataVec[1, 2, NA]
@assert dv[1] == 1
@assert dv[2] == 2
@assert isna(dv[3])
@assert isequal(eltype(dv), Int64)

#
# PooledDataVec's
#

pdv = PooledDataVec([1, 2, 3], falses(3))
@assert all(pdv .== [1, 2, 3])
@assert all(isna(pdv) .== falses(3))

@assert isequal(pdv, PooledDataVec([1, 2, 3], [false, false, false]))
@assert isequal(pdv, PooledDataVec([1, 2, 3]))

pdv = PooledDataVec(trues(3), falses(3))
@assert all(pdv .== [true, true, true])
@assert all(isna(pdv) .== falses(3))
@assert isequal(pdv, PooledDataVec(trues(3)))

pdv = PooledDataVec([1, 2, 3], falses(3))
@assert isequal(pdv, PooledDataVec(1:3))
@assert isequal(pdv, PooledDataVec(PooledDataVec([1, 2, 3])))

pdv = PooledDataVec(Int64, 3)
@assert isequal(eltype(pdv), Int64)
@assert all(isna(pdv) .== trues(3))

pdv = PooledDataVec(3)
@assert isequal(eltype(pdv), Float64)
@assert all(isna(pdv) .== trues(3))

pdv = PooledDataVec()
@assert isequal(eltype(pdv), Float64)
@assert all(isna(pdv) .== trues(0))

@assert isequal(pdvzeros(3), PooledDataVec(zeros(3)))
@assert isequal(pdvzeros(Int64, 3), PooledDataVec(zeros(Int64, 3)))
@assert isequal(pdvones(3), PooledDataVec(ones(3)))
@assert isequal(pdvones(Int64, 3), PooledDataVec(ones(Int64, 3)))
@assert isequal(pdvfalses(3), PooledDataVec(falses(3)))
@assert isequal(pdvtrues(3), PooledDataVec(trues(3)))

pdv = PooledDataVec[1, 2, NA]
@assert pdv[1] == 1
@assert pdv[2] == 2
@assert isna(pdv[3])
@assert isequal(eltype(pdv), Int64)

#
# DataMatrix
#

dm = DataMatrix([1 2; 3 4], falses(2, 2))
@assert isequal(dm.data, [1 2; 3 4])
@assert isequal(dm.na, falses(2, 2))

@assert isequal(dm, DataMatrix([1 2; 3 4], [false false; false false]))
@assert isequal(dm, DataMatrix([1 2; 3 4]))

dm = DataMatrix(trues(2, 2), falses(2, 2))
@assert isequal(dm.data, trues(2, 2))
@assert isequal(dm.na, falses(2, 2))

@assert isequal(dm, DataMatrix(trues(2, 2)))

#DataMatrix(dvzeros(3), dvzeros(3))
#DataMatrix(1:3, 1:3)

@assert isequal(DataMatrix([1 2; 3 4]), DataMatrix(DataMatrix([1 2; 3 4])))

dm = DataMatrix(Int64, 2, 2)
@assert isequal(eltype(dm), Int64)
@assert isequal(dm.na, trues(2, 2))

dm = DataMatrix(2, 2)
@assert isequal(eltype(dm), Float64)
@assert isequal(dm.na, trues(2, 2))

dm = DataMatrix(Int64)
@assert isequal(eltype(dm), Int64)
@assert isequal(dm.na, trues(0, 0))

dm = DataMatrix()
@assert isequal(eltype(dm), Float64)
@assert isequal(dm.na, trues(0, 0))

@assert isequal(dmzeros(2, 2), DataMatrix(zeros(2, 2)))
@assert isequal(dmzeros(Int64, 2, 2), DataMatrix(zeros(Int64, 2, 2)))

@assert isequal(dmones(2, 2), DataMatrix(ones(2, 2)))
@assert isequal(dmones(Int64, 2, 2), DataMatrix(ones(Int64, 2, 2)))

@assert isequal(dmfalses(2, 2), DataMatrix(falses(2, 2)))
@assert isequal(dmtrues(2, 2), DataMatrix(trues(2, 2)))

@assert isequal(dmeye(3, 2), DataMatrix(eye(3, 2)))
@assert isequal(dmeye(2), DataMatrix(eye(2)))
@assert isequal(dmdiagm([pi, pi]), DataMatrix(diagm([pi, pi])))

#
# DataFrame
#

df = DataFrame()
@assert isequal(df.columns, {})
# TODO: Get this to work
#@assert isequal(df.colindex, Index())

df = DataFrame({dvzeros(3), dvones(3)}, Index(["x1", "x2"]))
@assert nrow(df) == 3
@assert ncol(df) == 2

# TODO: Make isequal fail if colnames don't match
@assert isequal(df, DataFrame({dvzeros(3), dvones(3)}))
@assert isequal(df, DataFrame(quote x1 = [0.0, 0.0, 0.0]; x2 = [1.0, 1.0, 1.0] end))

@assert isequal(df, DataFrame([0.0 1.0; 0.0 1.0; 0.0 1.0], ["x1", "x2"]))
@assert isequal(df, DataFrame([0.0 1.0; 0.0 1.0; 0.0 1.0]))
@assert isequal(df, DataFrame(dvzeros(3), dvones(3)))

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
