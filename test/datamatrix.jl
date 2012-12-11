load("DataFrames")
using DataFrames

a = DataVec[1.0, 2.0, 3.0]
v_a = [1.0, 2.0, 3.0]

b = dmeye(3, 3)
m_b = eye(3, 3)

#
# Transposes
#
@assert all(a' .== v_a')
@assert all(a'' .== v_a'') #'
@assert all(b' .== m_b')
@assert all(b'' .== m_b'') #'

#
# DataVec * DataMatrix
#
@assert all(a * b[1, :] .== v_a * m_b[1, :])

#
# DataMatrix * DataVec
#
@assert all(b * a .== m_b * v_a)
@assert all(vector(b * a) .== m_b * v_a)

#
# DataMatrix * DataMatrix
#

@assert all(b * b .== m_b * m_b)

#
# DataVec * DataMatrix w/ NA's
#

b[1, 1] = NA
res = a * b[1, :]
@assert all(isna(res[:, 1]))
@assert all(!isna(res[:, 2]))
@assert all(!isna(res[:, 3]))
res = a * b[2, :]
@assert all(!isna(res))

#
# DataMatrix w NA's * DataVec
#

res = b * a
@assert isna(res[1])
@assert !isna(res[2])
@assert !isna(res[3])

#
# DataMatrix * DataMatrix
#

res = b * b
# 3x3 Float64 DataMatrix:
#  NA   NA   NA
#  NA  1.0  0.0
#  NA  0.0  1.0
@assert isna(res[1, 1])
@assert isna(res[1, 2])
@assert isna(res[1, 3])
@assert isna(res[2, 1])
@assert !isna(res[2, 2])
@assert !isna(res[2, 3])
@assert isna(res[3, 1])
@assert !isna(res[3, 2])
@assert !isna(res[3, 3])

res = b * dmeye(3)
# 3x3 Float64 DataMatrix:
#   NA   NA   NA
#  0.0  1.0  0.0
#  0.0  0.0  1.0
@assert isna(res[1, 1])
@assert isna(res[1, 2])
@assert isna(res[1, 3])
@assert !isna(res[2, 1])
@assert !isna(res[2, 2])
@assert !isna(res[2, 3])
@assert !isna(res[3, 1])
@assert !isna(res[3, 2])
@assert !isna(res[3, 3])

res = dmeye(3) * b
# julia> dmeye(3) * b
# 3x3 Float64 DataMatrix:
#  NA  0.0  0.0
#  NA  1.0  0.0
#  NA  0.0  1.0
@assert isna(res[1, 1])
@assert !isna(res[1, 2])
@assert !isna(res[1, 3])
@assert isna(res[2, 1])
@assert !isna(res[2, 2])
@assert !isna(res[2, 3])
@assert isna(res[3, 1])
@assert !isna(res[3, 2])
@assert !isna(res[3, 3])

# Test row operations
dm = dmeye(6, 2)
rowmeans(dm)

# Test column operations
dm = dmeye(6, 2)
colmeans(dm)

# Test linear algebra
du, dd, dv = svd(dmeye(3, 3))
u, d, v = svd(eye(3, 3))
@assert all(du .== u)
@assert all(dd .== d)
@assert all(dv .== v)

# Test elementary functions
dm = -dmeye(5, 5)
@assert all(abs(dm) .== eye(5, 5))
