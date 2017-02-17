using DataTables

dt = readtable(STDIN)

io = IOBuffer()
print(io, dt)
