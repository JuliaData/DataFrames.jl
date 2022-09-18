function Base.summary(io::IO, gd::GroupedDataFrame)
    N = length(gd)
    keystr = length(gd.cols) > 1 ? "keys" : "key"
    groupstr = N == 1 ? "group" : "groups"
    print(io, "GroupedDataFrame with $N $groupstr based on $keystr: ")
    join(io, groupcols(gd), ", ")
end

function Base.show(io::IO, gd::GroupedDataFrame;
                   allgroups::Bool = !get(io, :limit, false),
                   allrows::Bool = !get(io, :limit, false),
                   allcols::Bool = !get(io, :limit, false),
                   rowlabel::Symbol = :Row,
                   summary::Bool = true,
                   truncate::Int = 32,
                   kwargs...)
    # Check for keywords that are valid in other backends but not here.
    _verify_kwargs_for_text(; kwargs...)

    N = length(gd)

    summary && Base.summary(io, gd)

    if allgroups
        for i = 1:N
            nrows = size(gd[i], 1)
            rows = nrows > 1 ? "rows" : "row"

            identified_groups = [string(col, " = ", repr(gd[i][1, col]))
                                 for col in gd.cols]

            print(io, "\nGroup $i ($nrows $rows): ")
            join(io, identified_groups, ", ")
            println(io)

            show(io, gd[i]; summary=false,
                 allrows=allrows, allcols=allcols, rowlabel=rowlabel,
                 truncate=truncate, kwargs...)
        end
    else
        N > 0 || return


        (h, w) = get(io, :displaysize, displaysize(io))
        0 < h <= 3 && (h = 3) # show in full if h=0; show only headers and columns for small h>0

        h -= 2 # two lines are already used for header and gap between groups

        h1 = h2 = h # display heights available for first and last groups
        if !allrows && N > 1
            # line height of groups if printed in full (nrows + 3 extra for header)
            g1 = size(gd[1], 1) + 3
            g2 = size(gd[N], 1) + 3

            if g1 + g2 > h # won't fit on screen
                if g1 < h ÷ 2
                    h2 = h - g1 - 2 # show first group fully, squash last
                elseif g2 < h ÷ 2
                    h1 = h - g2 - 2 # show last group fully, squash first
                else
                    # squash both groups
                    h += 1
                    h1 = h ÷ 2
                    h2 = h - h1
                end
            end
        end


        nrows = size(gd[1], 1)
        rows = nrows > 1 ? "rows" : "row"

        identified_groups = [string(col, " = ", repr(gd[1][1, col]))
                             for col in gd.cols]

        print(io, "\nFirst Group ($nrows $rows): ")
        join(io, identified_groups, ", ")
        println(io)

        show(io, gd[1]; summary=false,
             allrows=allrows, allcols=allcols, rowlabel=rowlabel,
             truncate=truncate, kwargs..., display_size=(h1, w))


        N > 1 || return


        nrows = size(gd[N], 1)
        rows = nrows > 1 ? "rows" : "row"

        identified_groups = [string(col, " = ", repr(gd[N][1, col]))
                             for col in gd.cols]
        print(io, "\n⋮")
        print(io, "\nLast Group ($nrows $rows): ")
        join(io, identified_groups, ", ")
        println(io)

        show(io, gd[N]; summary=false,
             allrows=allrows, allcols=allcols, rowlabel=rowlabel,
             truncate=truncate, kwargs..., display_size=(h2, w))
    end
end

function Base.show(df::GroupedDataFrame;
                   allrows::Bool = !get(stdout, :limit, true),
                   allcols::Bool = !get(stdout, :limit, true),
                   allgroups::Bool = !get(stdout, :limit, true),
                   rowlabel::Symbol = :Row,
                   summary::Bool = true,
                   truncate::Int = 32,
                   kwargs...) # -> Nothing
    return show(stdout, df;
                allrows=allrows, allcols=allcols, allgroups=allgroups,
                rowlabel=rowlabel, summary=summary, truncate=truncate,
                kwargs...)
end
