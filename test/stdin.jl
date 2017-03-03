using DataTables
using CSV

dt = CSV.read(STDIN)

io = IOBuffer()
print(io, dt)
