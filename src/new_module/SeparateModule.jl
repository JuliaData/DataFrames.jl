module SeparateModule

using ..SeparateModuleIndex:
    AbstractIndex, AsTable, ColumnIndex, MultiColumnIndex, _names, make_unique, index
using DataAPI: All, Between, Cols, DataAPI, nrow
using InvertedIndices: InvertedIndices, Not
using Printf: @sprintf
using Tables: ByRow

import ..SeparateModuleIndex: funname

include("selection.jl")

export normalize_selection, make_pair_concrete, broadcast_pair

end
