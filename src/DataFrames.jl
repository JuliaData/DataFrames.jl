module DataFrames

using Base
import Base.length, Base.eltype, Base.ndims, Base.numel, Base.size, Base.promote, Base.promote_rule,
       Base.similar, Base.fill, Base.fill!, Base.one, Base.copy_to, Base.reshape,
       Base.convert, Base.reinterpret, Base.ref, Base.assign, Base.check_bounds,
       Base.push, Base.append!, Base.grow, Base.pop, Base.enqueue, Base.shift,
       Base.insert, Base.del, Base.del_all, Base.~, Base.-, Base.sign, Base.real,
       Base.imag, Base.conj!, Base.conj, Base.!, Base.+, Base.div, Base.mod,
       Base.-, Base.*, Base./, Base.^, Base.&, Base.|,
       Base.(./), Base.(.^), Base./, Base.\, Base.&, Base.|, Base.$, Base.(.*),
       Base.(.==), Base.==, Base.(.<), Base.<, Base.(.!=), Base.!=,
       Base.(.<=), Base.<= ,
       Base.>=, Base.<, Base.>,
       Base.order, Base.sort,
       Base.nnz, Base.find, Base.findn, Base.nonzeros,
       Base.areduce, Base.max, Base.min, Base.sum, Base.prod, Base.map_to,
       Base.filter, Base.transpose, Base.ctranspose, Base.permute, Base.hcat,
       Base.vcat, Base.cat, Base.isequal, Base.cumsum, Base.cumprod,
       Base.write, Base.read, Base.msync, Base.findn_nzs, Base.reverse,
       Base.iround, Base.itrunc, Base.ifloor, Base.iceil, Base.abs,
       Base.string, Base.show,
       Base.isnan, Base.isinf, Base.cmp, Base.sqrt, Base.min, Base.max, Base.isless, Base.atan2, Base.log,
       Base.start, Base.next, Base.done,
       Base.isempty, Base.expand,
       Base.map,
       Base.string,
       Base.intersect, Base.union,
       Base.bool,
       Base.print, Base.repl_show,
       Base.has, Base.get, Base.keys, Base.values,
       Base.copy, Base.deepcopy,
       Base.dump, Base.summary,
       Base.sub,
       Base.zeros


require("enum.jl")
require("bitarray.jl")

## require("options.jl")   ## to load the extras version
require("Options.jl")   ## to load the package version
using OptionsMod

## ---- index.jl ----
## Types
export AbstractIndex, Index, SimpleIndex
## Methods
export names!, replace_names!, replace_names, set_group, set_groups, get_groups, is_group

## ---- datavec.jl ----
## Types
export AbstractDataVec, NARule, DataVec, PooledDataVec, NAException, NAtype
## Methods
export isna, naRule, levels, indices,
       index_to_level, level_to_index,  # export needed?
       table,
       replace!,
       PooledDataVecs,  # The capitalization and/or name for this is a bit inconsistent (merge_pools, maybe?). Do we want to export?
       nafilter, nareplace, naFilter, naReplace,
       cut
## Specials
export NA,
       KEEP, FILTER, REPLACE,
       letters, LETTERS

## ---- namedarray.jl ----
## Types
export NamedArray

## ---- dataframe.jl ----
## Types
export AbstractDataFrame, DataFrame, SubDataFrame, GroupedDataFrame
## Methods
export colnames, names!, replace_names, replace_names!,
       nrow, ncol,
       # reconcile_groups,  
       index,
       set_group, set_groups, get_groups, rename_group!,
       head, tail,
       csvDataFrame,   # Inconsistent naming/capitalization?
       del!,  # Should we only have `del` to match Base.del?
       cbind, rbind,
       nas,
       with, within, within!, based_on,
       groupby, colwise, by,
       stack, unstack, merge,
       unique, complete_cases, duplicated,
       array, matrix,
       save, load_df

## ---- formula.jl ----
## Types
export Formula, ModelFrame, ModelMatrix
## Methods
export model_frame, model_matrix, interaction_design_matrix 
## all_interactions # looks internal to me. Uncomment if it should be exported.
     
## ---- utils.jl ----
## None of the methods in utils look like they should be exported.

## ---- indexing.jl ----
## Nothing is currently exported because this is still experimental. 
## The following shows what would be exported.
## ## Types
## export IndexedVec, Indexer
## ## Methods
## export in, between
 

load("DataFrames/src/index.jl")
load("DataFrames/src/datavec.jl")
load("DataFrames/src/namedarray.jl")
load("DataFrames/src/dataframe.jl")
load("DataFrames/src/formula.jl")
load("DataFrames/src/utils.jl")

## load("dlmread.jl")
## load("indexing.jl")

end # module DataFrames
