v = [1, 2, 3, 4]
dv = DataArray(v, falses(size(v)))

m = [1 2; 3 4]
dm = DataArray(m, falses(size(m)))

t = Array(Int, 2, 2, 2)
t[1:2, 1:2, 1:2] = 1
dt = DataArray(t, falses(size(t)))

dv = DataArray(v)
dv = DataArray(v, [false, false, false, false])

dv = DataArray(dv)
dm = DataArray(dm)
dt = DataArray(dt)

dv = DataArray(Int, 2)
dm = DataArray(Int, 2, 2)
dt = DataArray(Int, 2, 2, 2)

similar(dv)
similar(dm)
similar(dt)

similar(dv, 2)
similar(dm, 2, 2)
similar(dt, 2, 2, 2)
