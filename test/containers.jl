dv = dataones(3)
push!(dv, 3.0)
push!(dv, NA)

@assert isequal(dv, DataVector[1.0, 1.0, 1.0, 3.0, NA])

a, b = pop!(dv), pop!(dv)
@assert isna(a)
@assert b == 3.0

enqueue!(dv, 3.0)
enqueue!(dv, NA)

@assert isequal(dv, DataVector[NA, 3.0, 1.0, 1.0, 1.0])

a, b = shift(dv), shift(dv)
@assert isna(a)
@assert b == 3.0
