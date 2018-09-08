function Base.show(io::IO, r::DataFrameRow)
    labelwidth = mapreduce(n -> length(string(n)), max, _names(r)) + 2
    @printf(io, "DataFrameRow (row %d)\n", row(r))
    for (label, value) in pairs(r)
        println(io, rpad(label, labelwidth, ' '), something(value, ""))
    end
end
