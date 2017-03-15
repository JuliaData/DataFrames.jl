srand(1)
N = 1_000_000
v = randn(N)
dv = DataArray(v)
dvna = deepcopy(dv)
dvna[rand(1:N, 10_000)] = NA
idxv = shuffle([1:N])
idxdv = DataArray(idxv)

f1(v) = sum(v)
f2(v) = sum(dropna(v))
f3(v) = sum(dropna(v)) # Make this an iterator
f4(v) = mean(v)
f5(v) = mean(dropna(v))
f6(v) = mean(dropna(v)) # Make this an iterator
f7(v1, v2) = v1 + v2
f8(v1, v2) = v1 .> v2
f9(v, i) = v[i]

perf_test = Dict()

perf_test["sum(v): Vector with no NA's"] = () -> f1(v)
perf_test["sum(dv): DataVector with no NA's"] = () -> f1(dv)
perf_test["sum(dropna(dv)): DataVector with no NA's"] = () -> f2(dv)
perf_test["sum(*dropna(dv)): DataVector with no NA's"] = () -> f3(dv)

perf_test["sum(dvna): DataVector with NA's"] = () -> f4(dv)
perf_test["sum(dropna(dvna)): DataVector with NA's"] = () -> f5(dv)
perf_test["sum(*dropna(dvna)): DataVector with NA's"] = () -> f6(dv)

perf_test["mean(v): Vector with no NA's"] = () -> f4(v)
perf_test["mean(dv): DataVector with no NA's"] = () -> f4(dv)
perf_test["mean(dropna(dv)): DataVector with no NA's"] = () -> f5(dv)
perf_test["mean(*dropna(dv)): DataVector with no NA's"] = () -> f6(dv)

perf_test["mean(dvna): DataVector with NA's"] = () -> f4(dv)
perf_test["mean(dropna(dvna)): DataVector with NA's"] = () -> f5(dv)
perf_test["mean(*dropna(dvna)): DataVector with NA's"] = () -> f6(dv)

perf_test["v + 1.0 : Vector"] = () -> f7(v, 1.0)
perf_test["dv + 1.0 : DataVector with no NA's"] = () -> f7(dv, 1.0)
perf_test["dvna + 1.0 : DataVector with NA's"] = () -> f7(dvna, 1.0)

perf_test["v .> 1.0 : Vector"] = () -> f8(v, 1.0)
perf_test["dv .> 1.0 : DataVector with no NA's"] = () -> f8(dv, 1.0)
perf_test["dvna .> 1.0 : DataVector with NA's"] = () -> f8(dvna, 1.0)

perf_test["v[idxv] : Vector"] = () -> f9(v, idxv)
perf_test["dv[idxv] : DataVector and Vector indexing"] = () -> f9(dv, idxv)
perf_test["dv[idxdv] : DataVector and DataVector indexing"] = () -> f9(dv, idxdv)

for (name, f) in perf_test
    res = benchmark(f, "DataArray Operations", name, 10)
    # TODO: Keep permanent record
    printtable(res, header=false)
end
