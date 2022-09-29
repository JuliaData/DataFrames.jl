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

        (h, w) = displaysize(io)

        # in the code below we accept that output for desired height less
        # than 15 does not have to always exactly math the passed value
        if h > 0
            # 2 lines for header and gap between groups, 3 lines for prompts;
            # correcting for this allow at least 8 lines (4 for each group)
            h = max(h - 5, 8)

            if N == 1
                h1 = h + 3 # add two lines for prompts and one line as there no gap between groups
                h2 = 0 # not used
            else
                # line height of groups if printed in full; 4 lines for header
                # we assume scenario where eltype is printed for simplicity
                g1 = size(gd[1], 1) + 4
                g2 = size(gd[N], 1) + 4
                # below +2 is for 2 lines for prompts as we do not print summary
                if g1 + g2 > h # won't fit on screen
                    if g1 <= h ÷ 2
                        h1 = g1 + 2
                        h2 = h - g1 + 2 # show first group fully, squash last
                    elseif g2 <= h ÷ 2
                        h1 = h - g2 + 2 # show last group fully, squash first
                        h2 = g2 + 2
                    else
                        # squash both groups
                        h2 = h ÷ 2 + 2
                        h1 = h - h2 + 4
                    end
                else
                    h1 = g1 + 2
                    h2 = g2 + 2
                end
            end
        else
            h1 = h # no limit
            h2 = h # no limit
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
             truncate=truncate, display_size=(h1, w), kwargs...)

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
             truncate=truncate, display_size=(h2, w), kwargs...)
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
