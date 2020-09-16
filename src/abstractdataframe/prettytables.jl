# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#
# Description
# ==============================================================================
#
#     This file contains the functions that build the interface with
#     PrettyTables.jl.
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

using PrettyTables

# Internal function to print DataFrames using PrettyTables.jl.
function _pretty_table(io::IO, df::AbstractDataFrame;
                       allrows::Bool = !get(io, :limit, false),
                       allcols::Bool = !get(io, :limit, false),
                       rowlabel::Symbol = :Row,
                       summary::Bool = true,
                       eltypes::Bool = true,
                       rowid=nothing,
                       truncstring::Int = 32,
                       kwargs...)

    names = permutedims(propertynames(df))
    types = permutedims(compacttype.(eltype.(eachcol(df))))

    crop = :both

    if allcols && allrows
        crop = :none
    elseif allcols
        crop = :vertical
    elseif allrows
        crop = :horizontal
    end

    # Check if the user wants to display a summary about the DataFrame that is
    # being printed. This will be shown using the `title` option of
    # `pretty_table`.
    title = summary ? Base.summary(df) : ""

    # Create the formatter considering the current maximum size of the strings.
    _formatter = (v,i,j)->_df_formatter(v,i,j,truncstring)

    # Print the table with the selected options.
    pretty_table(io, df, vcat(names,types);
                 alignment                   = :l,
                 continuation_row_alignment  = :l,
                 crop                        = crop,
                 crop_num_lines_at_beginning = 2,
                 formatters                  = (_formatter,),
                 highlighters                = (_DF_HIGHLIGHTER,),
                 maximum_columns_width       = truncstring,
                 newline_at_end              = false,
                 nosubheader                 = !eltypes,
                 row_number_alignment        = :l,
                 row_number_column_title     = string(rowlabel),
                 show_row_number             = true,
                 tf                          = dataframe,
                 title                       = title,
                 vlines                      = [1])
end

################################################################################
#                             Auxiliary functions
################################################################################

# Default DataFrames highlighter for text backend.
#
# This highlighter change the text color to gray in cells with `nothing`,
# `missing`, `#undef`, and types related to DataFrames.jl.
function _df_h_f(data,i,j)
    try
        return ismissing(data[i,j]) ||
            data[i,j] == nothing ||
            typeof(data[i,j]) <: Union{AbstractDataFrame, GroupedDataFrame,
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

const _DF_HIGHLIGHTER = Highlighter(_df_h_f, Crayon(foreground = :dark_gray))

# Default DataFrames formatter for text backend.
#
# This formatter changes how the following types are presented when rendering
# the data frame:
#     - missing;
#     - nothing;
#     - Cells with types related to DataFrames.jl.

function _df_formatter(v,i,j,truncstring = 32)
    if typeof(v) <: Union{AbstractDataFrame, GroupedDataFrame, DataFrameRow,
                          DataFrameRows, DataFrameColumns}

        # Here, we must not use `print` or `show`. Otherwise, we can call
        # `_pretty_table` to render the current table leading to a stack
        # overflow.
        str = sprint(ourshow, v, truncstring, context = :compact => true)
        str = split(str, '\n')[1]
        return str
    elseif typeof(v) <: Unsigned
        # In case of an `Unsigned` value, use `show` to obtain the
        # representation instead of `print` that is used by PrettyTables.
        return sprint(show, v, context = :compact => true)
    elseif ismissing(v)
        return "missing"
    elseif v == nothing
        return ""
    else
        return v
    end
end
