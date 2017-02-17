#' @exported
#' @description
#'
#' Render a DataTableRow to an IO system. Each column of the DataTableRow
#' is printed on a separate line.
#'
#' @param io::IO The IO system where rendering will take place.
#' @param r::DataTableRow The DataTableRow to be rendered to `io`.
#'
#' @returns o::Void A `nothing` value.
#'
#' @examples
#'
#' dt = DataTable(A = 1:3, B = ["x", "y", "z"])
#' for r in eachrow(dt)
#'     show(STDOUT, r)
#' end
function Base.show(io::IO, r::DataTableRow)
    labelwidth = mapreduce(n -> length(string(n)), max, _names(r)) + 2
    @printf(io, "DataTableRow (row %d)\n", r.row)
    for (label, value) in r
        println(io, rpad(label, labelwidth, ' '), value)
    end
end
