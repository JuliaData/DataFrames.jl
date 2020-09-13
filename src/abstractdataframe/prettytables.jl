# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#
# Description
# ==============================================================================
#
#     This file contains the functions that build the interface with
#     PrettyTables.jl.
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

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
    _pretty_tables_conf[:maximum_columns_width]     = truncstring
    _pretty_tables_safeconf[:maximum_columns_width] = truncstring

    # Assemble the configurations for this print.
    _local_conf = deepcopy(_pretty_tables_conf)

    for kw in kwargs
        _local_conf[kw[1]] = kw[2]
    end

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
                     nt...)
    catch
        @warn """An unsupported argument was passed to PrettyTables.jl.
                 The default configuration will be used."""

        dictkeys = (collect(keys(_pretty_tables_safeconf))...,)
        dictvals = (collect(values(_pretty_tables_safeconf))...,)
        nt_sc = NamedTuple{dictkeys}(dictvals)

        pretty_table(io, df,
                     vcat(names,types);
                     crop = crop,
                     maximum_columns_width = truncstring,
                     nosubheader = !eltypes,
                     row_number_column_title = string(rowlabel),
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

# Default DataFrames formatter for text backend.
#
# This formatter changes how the following types are presented when rendering
# the data frame:
#     - missing;
#     - nothing;
#     - Cells with types related to DataFrames.jl.

function _df_f(v,i,j)
    if typeof(v) <: Union{AbstractDataFrame, GroupedDataFrame,
                          DataFrames.DataFrameRow, DataFrames.DataFrameRows,
                          DataFrames.DataFrameColumns}

        truncstring = haskey(_pretty_tables_conf, :maximum_columns_width) ?
            _pretty_tables_conf[:maximum_columns_width] : 32

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
