#' @exported
#' @description
#'
#' Render a DataFrameRow to an IO system. Each column of the DataFrameRow
#' is printed on a separate line.
#'
#' @param io::IO The IO system where rendering will take place.
#' @param r::DataFrameRow The DataFrameRow to be rendered to `io`.
#'
#' @returns `nothing` value.
#'
#' @examples
#'
#' df = DataFrame(A = 1:3, B = ["x", "y", "z"])
#' for r in eachrow(df)
#'     show(stdout, r)
#' end
function Base.show(io::IO, r::DataFrameRow)
    labelwidth = mapreduce(n -> length(string(n)), max, _names(r)) + 2
    @printf(io, "DataFrameRow (row %d)\n", row(r))
    for (label, value) in r
        println(io, rpad(label, labelwidth, ' '), something(value, ""))
    end
end

