module SeparateModuleIndex

using Unicode: Unicode
using REPL: REPL
using DataAPI: All, Between, Cols, DataAPI, nrow
using InvertedIndices: InvertedIndices, Not
using Printf: @sprintf
using Tables: ByRow

include("utils.jl")
include("index.jl")
include("selection.jl")

function index end

export
    _findall,
    _julia_charmap,
    _names,
    _norm_eq,
    AbstractIndex,
    add_names,
    AsTable,
    broadcast_pair,
    COLUMNINDEX_STR,
    ColumnIndex,
    funname,
    fuzzymatch,
    index,
    Index,
    make_pair_concrete,
    make_unique,
    make_unique!,
    MULTICOLUMNINDEX_STR,
    MULTICOLUMNINDEX_TUPLE,
    MultiColumnIndex,
    normalize_selection,
    parentcols,
    rename!,
    SubIndex,
    SymbolOrString

end
