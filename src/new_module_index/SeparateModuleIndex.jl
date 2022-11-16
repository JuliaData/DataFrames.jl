module SeparateModuleIndex
using Unicode: Unicode
using REPL: levenshtein
using DataAPI: All, Between, Cols, DataAPI, nrow
using InvertedIndices: InvertedIndices, Not
using Printf: @sprintf

include("utils.jl")
include("index.jl")

# new def here; the above files are copied as is (stuff removed from utils.jl)
function index end

export
    _findall, add_names, funname, SubIndex, _names,
    AsTable, ColumnIndex, MultiColumnIndex, Index,
    COLUMNINDEX_STR, MULTICOLUMNINDEX_STR, SymbolOrString, AbstractIndex,
    MULTICOLUMNINDEX_TUPLE, make_unique, index, parentcols,rename!,fuzzymatch, make_unique!, _norm_eq, _julia_charmap
end
