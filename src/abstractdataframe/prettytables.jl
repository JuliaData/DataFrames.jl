##############################################################################
##
## Functions related to the interface with PrettyTables.jl.
##
##############################################################################

# Default DataFrames formatter for text backend.
#
# This formatter changes how the following types are presented when rendering
# the data frame:
#     - missing;
#     - nothing;
#     - Cells with types related to DataFrames.jl.

function _pretty_tables_general_formatter(v, i::Integer, j::Integer)
    if typeof(v) <: Union{AbstractDataFrame, GroupedDataFrame, DataFrameRow,
                          DataFrameRows, DataFrameColumns}

        # Here, we must not use `print` or `show`. Otherwise, we will call
        # `_pretty_table` to render the current table leading to a stack
        # overflow.
        return sprint(summary, v)
    elseif ismissing(v)
        return "missing"
    elseif v === nothing
        return ""
    else
        return v
    end
end

# Default DataFrames highlighter for text backend.
#
# This highlighter changes the text color to gray in cells with `nothing`,
# `missing`, `#undef`, and types related to DataFrames.jl.
function _pretty_tables_highlighter_func(data, i::Integer, j::Integer)
    try
        cell = data[i, j]
        return ismissing(cell) ||
            cell === nothing ||
            cell isa Union{AbstractDataFrame, GroupedDataFrame,
                           DataFrameRow, DataFrameRows,
                           DataFrameColumns}
    catch e
        if isa(e, UndefRefError)
            return true
        else
            rethrow(e)
        end
    end
end

# Constants for the HTML backend.
const _PRETTY_TABLES_HTML_FORMATTER = [_pretty_tables_general_formatter]

const _PRETTY_TABLES_HTML_HIGHLIGHTER = [HtmlHighlighter(_pretty_tables_highlighter_func,
                                                         ["font-style" => "italic"])]

const _PRETTY_TABLES_HTML_TABLE_STYLE = HtmlTableStyle(first_line_column_label = Pair{String, String}[],
                                                       table = ["margin-bottom" => "6px"])

# Constants for the text backend.
const _PRETTY_TABLES_TEXT_FORMATTER = [_pretty_tables_general_formatter]

const _PRETTY_TABLES_TEXT_HIGHLIGHTER = [TextHighlighter(_pretty_tables_highlighter_func,
                                                         Crayon(foreground = :dark_gray))]

const _PRETTY_TABLES_TEXT_TABLE_FORMAT = TextTableFormat(; @text__no_horizontal_lines,
                                                         @text__no_vertical_lines,
                                                         ellipsis_line_skip = 3,
                                                         horizontal_line_after_column_labels = true,
                                                         horizontal_line_before_summary_rows = true,
                                                         vertical_line_after_row_label_column = true,
                                                         vertical_line_after_row_number_column = true)

const _PRETTY_TABLES_TEXT_TABLE_STYLE = TextTableStyle(row_label = Crayon())

