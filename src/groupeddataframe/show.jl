function Base.show(io::IO, gd::GroupedDataFrame;
                   allgroups::Bool = !get(io, :limit, false),
                   allrows::Bool = !get(io, :limit, false),
                   allcols::Bool = !get(io, :limit, false),
                   splitcols::Bool = get(io, :limit, false),
                   rowlabel::Symbol = :Row,
                   summary::Bool = true)
    N = length(gd)
    keynames = names(gd.parent)[gd.cols]
    parent_names = names(gd.parent)
    keys = join(string.(keynames), ", ")

    keystr = length(gd.cols) > 1 ? "keys" : "key"
    groupstr = N == 1 ? "group" : "groups"
    summary && print(io, "$(typeof(gd).name) with $N $groupstr based on $keystr: $keys")
    if allgroups
        for i = 1:N
            nrows = size(gd[i], 1)
            rows = nrows > 1 ? "rows" : "row"

            identified_groups = [string(parent_names[col], " = ", repr(gd[i][1, col]))
                                 for col in gd.cols]

            print(io, "\nGroup $i ($nrows $rows): ")
            join(io, identified_groups, ", ")

            show(io, gd[i], summary=false,
                 allrows=allrows, allcols=allcols, rowlabel=rowlabel)
        end
    else
        if N > 0
            nrows = size(gd[1], 1)
            rows = nrows > 1 ? "rows" : "row"

            identified_groups = [string(parent_names[col], " = ", repr(gd[1][1, col]))
                                 for col in gd.cols]

            print(io, "\nFirst Group ($nrows $rows): ")
            join(io, identified_groups, ", ")

            show(io, gd[1], summary=false,
                 allrows=allrows, allcols=allcols, rowlabel=rowlabel)
        end
        if N > 1
            nrows = size(gd[N], 1)
            rows = nrows > 1 ? "rows" : "row"

            identified_groups = [string(parent_names[col], " = ", repr(gd[N][1, col]))
                                 for col in gd.cols]
            print(io, "\n⋮")
            print(io, "\nLast Group ($nrows $rows): ")
            join(io, identified_groups, ", ")

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
