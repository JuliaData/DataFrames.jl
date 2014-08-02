a = eye(100)
b = eye(100)

dm_a = data(a)
dm_b = data(b)

dm_a_na = copy(dm_a)
dm_a_na[:, :] = NA
dm_b_na = copy(dm_b)
dm_b_na[:, :] = NA

f1() = *(a, b)
f2() = *(dm_a, dm_b)
f3() = *(dm_a_na, dm_b_na)

df1 = benchmark(f1,
                "Linear Algebra",
                "Matrix Multiplication w/ No NA's",
                1_000)
df2 = benchmark(f2,
                "Linear Algebra",
                "DataMatrix Multiplication w/ No NA's",
                1_000)
df3 = benchmark(f3,
                "Linear Algebra",
                "DataMatrix Multiplication w/ NA's",
                1_000)

# TODO: Keep permanent record
printtable(vcat(df1, df2, df3), header=false)

# Compare with R
# We're 10x as fast!
# a <- diag(100)
# b <- diag(100)
# a %*% b
# s <- Sys.time(); a %*% b; e <- Sys.time(); e - s
