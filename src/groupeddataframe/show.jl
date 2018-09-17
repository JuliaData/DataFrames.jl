function Base.show(io::IO, gd::GroupedDataFrame;
                   allgroups::Bool = !get(io, :limit, false),
                   allrows::Bool = !get(io, :limit, false),
                   allcols::Bool = !get(io, :limit, false),
                   splitcols::Bool = get(io, :limit, false),
                   rowlabel::Symbol = :Row,
                   summary::Bool = true)
    N = length(gd)
    keystr = N > 1 ? "keys" : "key"
    keys = join(':' .* string.([:A, :B]), ", ")
    summary && print(io, "$(typeof(gd)) with $N groups based on $keystr: $keys")
    if allgroups
        for i = 1:N
            nrows = size(gd[i], 1)
            rows = nrows > 1 ? "rows" : "row"
            print(io, "\nGroup $i: $nrows $rows")
            show(io, gd[i], summary=false,
                 allrows=allrows, allcols=allcols, rowlabel=rowlabel)
        end
    else
        if N > 0
            nrows = size(gd[1], 1)
            rows = nrows > 1 ? "rows" : "row"
            print(io, "\nFirst Group: $nrows $rows")
            show(io, gd[1], summary=false,
                 allrows=allrows, allcols=allcols, rowlabel=rowlabel)
        end
        if N > 1
            print(io, "\n⋮\n")
            nnrows = size(gd[N], 1)
            rows = nrows > 1 ? "rows" : "row"
            print(io, "Last Group: $nrows $rows")
            show(io, gd[N], summary=false,
                 allrows=allrows, allcols=allcols, rowlabel=rowlabel)
        end
    end
end

function Base.show(df::GroupedDataFrame;
                   allrows::Bool = !get(stdout, :limit, true),
                   allcols::Bool = !get(stdout, :limit, true),
                   allgroups::Bool = !get(stdout, :limit, true),
                   splitcols::Bool = get(stdout, :limit, true),
                   rowlabel::Symbol = :Row,
                   summary::Bool = true) # -> Nothing
    return show(stdout, df,
                allrows=allrows, allcols=allcols, allgroups=allgroups, splitcols=splitcols,
                rowlabel=rowlabel, summary=summary)
end