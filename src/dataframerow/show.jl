function Base.show(io::IO, r::DataFrameRow)
    labelwidth = mapreduce(n -> length(string(n)), max, _names(r)) + 2
    @printf(io, "DataFrameRow (row %d)", row(r))
    for (label, value) in pairs(r)
        print(io, '\n', rpad(label, labelwidth, ' '), something(value, ""))
    end
end
