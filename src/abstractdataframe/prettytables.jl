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

    names = reshape( propertynames(df), (1,:) )
    types = DataFrames.compacttype.(reshape( eltype.(eachcol(df)), (1,:) ))

    crop = :both

    if allcols && allrows
        crop = :none
    elseif allcols
        crop = :vertical
    elseif allrows
        crop = :horizontal
    end

    # Update the maximum column width. This is necessary because the default
    # formatter must have access to the option we are using.
    _PRETTY_TABLES_CONF[:maximum_columns_width]     = truncstring
    _PRETTY_TABLES_SAFECONF[:maximum_columns_width] = truncstring

    # Assemble the configurations for this print.
    _local_conf = deepcopy(_PRETTY_TABLES_CONF)

    for kw in kwargs
        _local_conf[kw[1]] = kw[2]
    end

    # Check if the user wants to display a summary about the DataFrame that is
    # being printed. This will be shown using the `title` option of
    # `pretty_table`.
    title = summary ? Base.summary(df) : ""

    # Transform into a named tuple so that it can be passed to PrettyTables.jl.
    dictkeys = (collect(keys(_local_conf))...,)
    dictvals = (collect(values(_local_conf))...,)
    nt = NamedTuple{dictkeys}(dictvals)

    # Print the table with the selected options.
    try
        pretty_table(io, df,
                     vcat(names,types);
                     crop = crop,
                     nosubheader = !eltypes,
                     row_number_column_title = string(rowlabel),
                     title = title,
                     nt...)
    catch
        @warn """An unsupported argument was passed to PrettyTables.jl.
                 The default configuration will be used."""

        dictkeys = (collect(keys(_PRETTY_TABLES_SAFECONF))...,)
        dictvals = (collect(values(_PRETTY_TABLES_SAFECONF))...,)
        nt_sc = NamedTuple{dictkeys}(dictvals)

        pretty_table(io, df,
                     vcat(names,types);
                     crop = crop,
                     maximum_columns_width = truncstring,
                     nosubheader = !eltypes,
                     row_number_column_title = string(rowlabel),
                     title = title,
                     nt_sc...)
    end
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
                                       DataFrames.DataFrameRow,
                                       DataFrames.DataFrameRows,
                                       DataFrames.DataFrameColumns}
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

function _df_formatter(v,i,j)
    if typeof(v) <: Union{AbstractDataFrame, GroupedDataFrame, DataFrameRow,
                          DataFrameRows, DataFrameColumns}

        truncstring = haskey(_PRETTY_TABLES_CONF, :maximum_columns_width) ?
            _PRETTY_TABLES_CONF[:maximum_columns_width] : 32

        # Here, we must not use `print` or `show`. Otherwise, we can call
        # `_pretty_table` to render the current table leading to a stack
        # overflow.
        str = sprint(ourshow, v, truncstring, context = :compact => true)
        str = split(str, '\n')[1]
        return str
    elseif ismissing(v)
        return "missing"
    elseif v == nothing
        return ""
    else
        return v
    end
end
