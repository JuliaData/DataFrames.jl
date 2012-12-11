load("DataFrames")
using DataFrames

a = dmeye(100)
b = dmeye(100)

*(a, b)
DataFrames.naive_mult(a, b)

N = 10
@elapsed for itr in 1:N
	*(a, b)
end
@elapsed for itr in 1:N
	DataFrames.naive_mult(a, b)
end

# Compare with R
# We're 10x as fast!
# a <- diag(100)
# b <- diag(100)
# a %*% b

# s <- Sys.time(); a %*% b; e <- Sys.time(); e - s
