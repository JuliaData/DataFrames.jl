module SeparateModule

import ..SeparateModuleIndex: funname

using ..DataFrames: AbstractDataFrame, DataFrame, DataFrameRow, SubDataFrame
using ..SeparateModuleIndex:
    AbstractIndex, AsTable, ColumnIndex, MultiColumnIndex, _names, funname, make_unique
using DataAPI: All, Between, Cols, DataAPI, nrow
using InvertedIndices: InvertedIndices, Not
using Printf: @sprintf
using Tables: ByRow


# include("index.jl")
include("selection.jl")



export normalize_selection, make_pair_concrete, broadcast_pair, _manipulate
end
