using DataFrames

df = readtable(STDIN)

io = IOBuffer()
print(io, df)
