function Base.show(io::IO, gd::GroupedDataFrame)
    N = length(gd)
    println(io, "$(typeof(gd))  $N groups with keys: $(gd.cols)")
    println(io, "First Group:")
    show(io, gd[1])
    if N > 1
        print(io, "\n⋮\n")
        println(io, "Last Group:")
        show(io, gd[N])
    end
end

function Base.showall(io::IO, gd::GroupedDataFrame)
    N = length(gd)
    println(io, "$(typeof(gd))  $N groups with keys: $(gd.cols)")
    for i = 1:N
        println(io, "gd[$i]:")
        show(io, gd[i])
    end
end
