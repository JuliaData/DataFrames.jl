function Base.show(io::IO, gd::GroupedDataFrame;
                   allgroups::Bool = false,
                   allrows::Bool = false,
                   allcols::Bool = false,
                   rowlabel::Symbol = :Row,
                   summary::Bool = true)
    N = length(gd)
    println(io, "$(typeof(gd))  $N groups with keys: $(gd.cols)")
    if allrows
        for i = 1:N
            println(io, "gd[$i]:")
            show(io, gd[i],
                 allrows=allrows, allcols=allcols, rowlabel=rowlabel, summary=summary)
        end
    else
        if N > 0
            println(io, "First Group:")
            show(io, gd[1],
                 allrows=allrows, allcols=allcols, rowlabel=rowlabel, summary=summary)
        end
        if N > 1
            print(io, "\n⋮\n")
            println(io, "Last Group:")
            show(io, gd[N],
                 allrows=allrows, allcols=allcols, rowlabel=rowlabel, summary=summary)
        end
    end
end

function Base.show(df::GroupedDataFrame;
                   allrows::Bool = !get(io, :limit, false),
                   allcols::Bool = !get(io, :limit, false),
                   allgroups::Bool = !get(io, :limit, false),
                   rowlabel::Symbol = :Row,
                   summary::Bool = true) # -> Nothing
    return show(stdout, df,
                allrows=allrows, allcols=allcols, allgroups=allgroups,
                rowlabel=rowlabel, summary=summary)
end