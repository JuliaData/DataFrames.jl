load("Benchmark")
using Benchmark

load("DataFrames")
using DataFrames

a = eye(100)
b = eye(100)

dm_a = dmeye(100)
dm_b = dmeye(100)

dm_a_na = deepcopy(dm_a)
dm_a_na[:, :] = NA
dm_b_na = deepcopy(dm_b)
dm_b_na[:, :] = NA

f1() = *(a, b)
f2() = *(dm_a, dm_b)
f3() = *(dm_a_na, dm_b_na)

df1 = benchmark(f1, "Linear Algebra", "Matrix Multiplication w/ No NA's", 10)
df2 = benchmark(f2, "Linear Algebra", "DataMatrix Multiplication w/ No NA's", 10)
df3 = benchmark(f3, "Linear Algebra", "DataMatrix Multiplication w/ NA's", 10)

print_table(rbind(df1, df2, df3))

# Compare with R
# We're 10x as fast!
# a <- diag(100)
# b <- diag(100)
# a %*% b
# s <- Sys.time(); a %*% b; e <- Sys.time(); e - s
