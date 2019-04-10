function Base.show(io::IO, dfr::DataFrameRow;
                   allcols::Bool = !get(io, :limit, false),
                   splitcols = get(io, :limit, false),
                   rowlabel::Symbol = :Row)
    r, c = parentindices(dfr)
    print(io, "DataFrameRow")
    _show(io, view(parent(dfr), [r], c), allcols=allcols, splitcols=splitcols,
          rowlabel=rowlabel, summary=false, rowid=r)
end

Base.show(dfr::DataFrameRow;
          allcols::Bool = !get(stdout, :limit, true),
          splitcols = get(stdout, :limit, true),
          rowlabel::Symbol = :Row) =
    show(stdout, dfr, allcols=allcols, splitcols=splitcols, rowlabel=rowlabel)
