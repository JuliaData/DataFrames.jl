module SeparateModuleIndex
using DataAPI: All, Between, Cols, DataAPI, nrow
using InvertedIndices: InvertedIndices, Not
using Printf: @sprintf

include("utils.jl")
include("index.jl")

# new def here; the above files are copied as is (stuff removed from utils.jl)
index(df) = throw("struct needs to define this")

export
    _findall, add_names, funname, SubIndex, _names,
    AsTable, ColumnIndex, MultiColumnIndex, Index,
    COLUMNINDEX_STR, MULTICOLUMNINDEX_STR, SymbolOrString, AbstractIndex,
    MULTICOLUMNINDEX_TUPLE, make_unique, index,parentcols,rename!,fuzzymatch
end
