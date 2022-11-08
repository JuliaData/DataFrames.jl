module SeparateModule

using ..SeparateModuleIndex: AbstractIndex,ColumnIndex,MultiColumnIndex, make_unique, AsTable,_names,funname
import ..SeparateModuleIndex: funname


using Printf: @sprintf
using InvertedIndices: Not, InvertedIndices
using DataAPI: Between, All, Cols, nrow, DataAPI
using ..DataFrames: AbstractDataFrame, DataFrame, DataFrameRow, SubDataFrame
using Tables: ByRow


# include("index.jl")
include("selection.jl")



export normalize_selection , make_pair_concrete,  broadcast_pair,_manipulate
end
