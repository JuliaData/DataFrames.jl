function Base.show(io::IO, dfr::DataFrameRow;
                   allcols::Bool = !get(io, :limit, false),
                   rowlabel::Symbol = :Row,
                   eltypes::Bool = true,
                   truncate::Int = 32)
    r, c = parentindices(dfr)
    _show(io, view(parent(dfr), [r], c), allcols=allcols, rowlabel=rowlabel,
          summary=false, rowid=r, eltypes=eltypes, truncate=truncate,
          title = "DataFrameRow")
end

Base.show(io::IO, mime::MIME"text/plain", dfr::DataFrameRow;
          allcols::Bool = !get(io, :limit, false),
          rowlabel::Symbol = :Row,
          eltypes::Bool = true,
          truncate::Int = 32) =
    show(io, dfr, allcols=allcols, rowlabel=rowlabel, eltypes=eltypes,
         truncate=truncate)

Base.show(dfr::DataFrameRow;
          allcols::Bool = !get(stdout, :limit, true),
          rowlabel::Symbol = :Row,
          eltypes::Bool = true,
          truncate::Int = 32) =
    show(stdout, dfr, allcols=allcols, rowlabel=rowlabel, eltypes=eltypes,
         truncate=truncate)
