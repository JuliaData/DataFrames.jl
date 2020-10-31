##############################################################################
##
## Functions related to the interface with PrettyTables.jl.
##
##############################################################################

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

const _PRETTY_TABLES_HIGHLIGHTER = Highlighter(_pretty_tables_highlighter_func,
                                               Crayon(foreground = :dark_gray))

# Default DataFrames formatter for text backend.
#
# This formatter changes how the following types are presented when rendering
# the data frame:
#     - missing;
#     - nothing;
#     - Cells with types related to DataFrames.jl.

function _pretty_tables_general_formatter(v, i, j)
    if typeof(v) <: Union{AbstractDataFrame, GroupedDataFrame, DataFrameRow,
                          DataFrameRows, DataFrameColumns}

        # Here, we must not use `print` or `show`. Otherwise, we will call
        # `_pretty_table` to render the current table leading to a stack
        # overflow.
        str = sprint(summary, v)
        return str
    elseif ismissing(v)
        return "missing"
    elseif v === nothing
        return ""
    else
        return v
    end
end

# Formatter to align the floating points as in Julia array printing.
#
# - `float_cols` contains the IDs of the columns that must be formatted.
# - `max_pad` is a vector containing the maximum padding in each columns.

function _pretty_tables_float_formatter(v, i, j, float_cols, max_pad)
    # We apply this formatting only to the columns that contains only floats.
    ind = findfirst(x -> x == j, float_cols)

    if !(ind === nothing)
        # Get the order of the current number.
        order = v isa Number ? floor(Int, log10(v)) : 0

        # If the order is greater than 5, then the number is printed using
        # scientific notation. This must be taken into account when computing
        # the paddding.
        order > 5 && (order = 0)

        # Compute the required pad.
        max_pad_i = max_pad[ind]
        pad = clamp(max_pad_i - order, 0, max_pad_i)

        # Return the formatted number.
        str = sprint(print, v)
        return " "^pad * str
    else
        return v
    end
end
