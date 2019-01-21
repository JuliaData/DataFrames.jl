function Base.show(io::IO, dfr::DataFrameRow;
                   allcols::Bool = !get(io, :limit, false),
                   splitcols = get(io, :limit, false),
                   rowlabel::Symbol = :Row,
                   missingstring::AbstractString = "missing")
    r, c = parentindices(dfr)
    print(io, "DataFrameRow")
    _show(io, view(parent(dfr), [r], c), allcols=allcols, splitcols=splitcols,
          rowlabel=rowlabel, summary=false, missingstring=missingstring, rowid=r)
end

Base.show(dfr::DataFrameRow;
          allcols::Bool = !get(io, :limit, true),
          splitcols = get(io, :limit, true),
          rowlabel::Symbol = :Row,
          missingstring::AbstractString = "missing") =
    show(stdout, dfr, allcols=allcols, splitcols=splitcols, rowlabel=rowlabel, missingstring=missingstring)
