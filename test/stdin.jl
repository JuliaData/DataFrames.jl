using DataTables

df = readtable(STDIN)

io = IOBuffer()
print(io, df)
