function Base.show(io::IO, dfr::DataFrameRow;
                   allcols::Bool = !get(io, :limit, false),
                   rowlabel::Symbol = :Row,
                   eltypes::Bool = true,
                   truncate::Int = 32,
                   kwargs...)
    # Check for keywords that are valid in other backends but not here.
    _verify_kwargs_for_text(; kwargs...)

    r, c = parentindices(dfr)
    _show(io, view(parent(dfr), [r], c); allcols=allcols, rowlabel=rowlabel,
          summary=false, rowid=r, eltypes=eltypes, truncate=truncate,
          title="DataFrameRow", kwargs...)
end

Base.show(io::IO, mime::MIME"text/plain", dfr::DataFrameRow;
          allcols::Bool = !get(io, :limit, false),
          rowlabel::Symbol = :Row,
          eltypes::Bool = true,
          truncate::Int = 32,
          kwargs...) =
    show(io, dfr; allcols=allcols, rowlabel=rowlabel, eltypes=eltypes,
         truncate=truncate, kwargs...)

Base.show(dfr::DataFrameRow;
          allcols::Bool = !get(stdout, :limit, true),
          rowlabel::Symbol = :Row,
          eltypes::Bool = true,
          truncate::Int = 32,
          kwargs...) =
    show(stdout, dfr; allcols=allcols, rowlabel=rowlabel, eltypes=eltypes,
         truncate=truncate, kwargs...)

function Base.show(io::IO, mime::MIME"text/html", dfr::DataFrameRow; kwargs...)
    _verify_kwargs_for_html(; kwargs...)
    r, c = parentindices(dfr)
    title = "DataFrameRow ($(length(dfr)) columns)"
    _show(io, mime, view(parent(dfr), [r], c); rowid=r, title=title, kwargs...)
end

function Base.show(io::IO, mime::MIME"text/latex", dfr::DataFrameRow; eltypes::Bool=true)
    r, c = parentindices(dfr)
    _show(io, mime, view(parent(dfr), [r], c), eltypes=eltypes, rowid=r)
end

function Base.show(io::IO, mime::MIME"text/csv", dfr::DataFrameRow)
    r, c = parentindices(dfr)
    show(io, mime, view(parent(dfr), [r], c))
end

function Base.show(io::IO, mime::MIME"text/tab-separated-values", dfr::DataFrameRow)
    r, c = parentindices(dfr)
    show(io, mime, view(parent(dfr), [r], c))
end
