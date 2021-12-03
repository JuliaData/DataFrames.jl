# precompile.jl contains precompilation directives for all methods whose compilation
# is triggered by running DataFrames tests and takes more than a given threshold
# of 0.1.
# `precompile` calls are generated using SnoopCompile via:
#     using SnoopCompileCore
#     inf_timing = @snoopi tmin=0.1 include("test/runtests.jl")
#     using SnoopCompile
#     pc = SnoopCompile.parcel(inf_timing)
#     SnoopCompile.write("src/other/precompile_tmp.jl", pc[:DataFrames], always=true)
# and then hand edited. The editing steps are:
# * removing signatures with anonymous functions
# * removing signatures with specific NamedTuples passed as arguments except `x1` as name or kwargs
#   (which is generated internally)
# * changing Int64 to Int (to handle 32-bit architectures correctly)
# * disabling precompilation on Julia older than 1.5
# * run @warnpcfail check for all=true and all=false both on Julia stable and nightly

function precompile(all=false)
    VERSION >= v"1.5" || return nothing

        all || ccall(:jl_generating_output, Cint, ()) == 1 || return nothing

        Base.precompile(Tuple{Aggregate{typeof(std), Nothing},Vector{Union{Missing, Int}},GroupedDataFrame{DataFrame}})
        Base.precompile(Tuple{Core.kwftype(typeof(describe)),NamedTuple{(:cols,), Tuple{Vector{Pair{Symbol, Pair{Function, Symbol}}}}},typeof(describe),DataFrame})
        Base.precompile(Tuple{Core.kwftype(typeof(innerjoin)),NamedTuple{(:on, :makeunique, :validate, :renamecols), Tuple{Symbol, Bool, Pair{Bool, Bool}, Pair{String, String}}},typeof(innerjoin),DataFrame,DataFrame})
        Base.precompile(Tuple{Core.kwftype(typeof(innerjoin)),NamedTuple{(:on, :makeunique, :validate, :renamecols), Tuple{Symbol, Bool, Pair{Bool, Bool}, Pair{String, Symbol}}},typeof(innerjoin),DataFrame,DataFrame})
        Base.precompile(Tuple{Core.kwftype(typeof(innerjoin)),NamedTuple{(:on, :makeunique, :validate, :renamecols), Tuple{Symbol, Bool, Pair{Bool, Bool}, Pair{Symbol, Symbol}}},typeof(innerjoin),DataFrame,DataFrame})
        Base.precompile(Tuple{Core.kwftype(typeof(innerjoin)),NamedTuple{(:on, :makeunique, :validate, :renamecols), Tuple{Vector{Any}, Bool, Pair{Bool, Bool}, Pair{String, Symbol}}},typeof(innerjoin),DataFrame,DataFrame})
        Base.precompile(Tuple{Core.kwftype(typeof(innerjoin)),NamedTuple{(:on, :renamecols), Tuple{Vector{Any}, Pair{String, String}}},typeof(innerjoin),DataFrame,DataFrame})
        Base.precompile(Tuple{Core.kwftype(typeof(innerjoin)),NamedTuple{(:on,), Tuple{Symbol}},typeof(innerjoin),DataFrame,DataFrame})
        Base.precompile(Tuple{Core.kwftype(typeof(leftjoin!)),NamedTuple{(:on, :makeunique, :source), Tuple{Pair{Symbol, Symbol}, Bool, Symbol}},typeof(leftjoin!),DataFrame,DataFrame})
        Base.precompile(Tuple{Core.kwftype(typeof(leftjoin)),NamedTuple{(:on, :makeunique, :validate, :renamecols), Tuple{Vector{Any}, Bool, Pair{Bool, Bool}, Pair{Symbol, String}}},typeof(leftjoin),DataFrame,DataFrame})
        Base.precompile(Tuple{Core.kwftype(typeof(leftjoin)),NamedTuple{(:on, :renamecols, :source), Tuple{Vector{Any}, Pair{String, String}, Symbol}},typeof(leftjoin),DataFrame,DataFrame})
        Base.precompile(Tuple{Core.kwftype(typeof(outerjoin)),NamedTuple{(:on, :makeunique, :validate, :renamecols), Tuple{Vector{Any}, Bool, Pair{Bool, Bool}, Pair{Symbol, Symbol}}},typeof(outerjoin),DataFrame,DataFrame})
        Base.precompile(Tuple{Core.kwftype(typeof(outerjoin)),NamedTuple{(:on, :source, :makeunique), Tuple{Symbol, String, Bool}},typeof(outerjoin),DataFrame,DataFrame})
        Base.precompile(Tuple{Core.kwftype(typeof(outerjoin)),NamedTuple{(:on, :source, :makeunique), Tuple{Symbol, Symbol, Bool}},typeof(outerjoin),DataFrame,DataFrame})
        Base.precompile(Tuple{Core.kwftype(typeof(reduce)),NamedTuple{(:cols, :source), Tuple{Symbol, Nothing}},typeof(reduce),typeof(vcat),NTuple{5, DataFrame}})
        Base.precompile(Tuple{Core.kwftype(typeof(select!)),NamedTuple{(:renamecols,), Tuple{Bool}},typeof(select!),GroupedDataFrame{SubDataFrame{DataFrame, SubIndex{Index, Vector{Int}, Vector{Int}}, UnitRange{Int}}},Union{Regex, AbstractString, Function, Signed, Symbol, Unsigned, Pair, Type, All, Between, Cols, InvertedIndex, AbstractVecOrMat{T} where T}})
        Base.precompile(Tuple{Core.kwftype(typeof(show)),NamedTuple{(:allrows, :allcols, :allgroups, :rowlabel, :summary, :truncate), Tuple{Bool, Bool, Bool, Symbol, Bool, Int}},typeof(show),Base.PipeEndpoint,GroupedDataFrame{DataFrame}})
        Base.precompile(Tuple{Core.kwftype(typeof(show)),NamedTuple{(:show_row_number,), Tuple{Bool}},typeof(show),IOContext{IOBuffer},DataFrame})
        Base.precompile(Tuple{Core.kwftype(typeof(vcat)),NamedTuple{(:cols, :source), Tuple{Symbol, Symbol}},typeof(vcat),DataFrame,Vararg{DataFrame}})
        Base.precompile(Tuple{Core.kwftype(typeof(vcat)),NamedTuple{(:cols,), Tuple{Vector{Symbol}}},typeof(vcat),DataFrame,Vararg{DataFrame}})
        Base.precompile(Tuple{Type{DataFrame},Vector{Tuple{Int, Int}}})
        Base.precompile(Tuple{typeof(_innerjoin_sorted),OnCol{Tuple{Vector{Int}, Vector{String}}, 2},OnCol{Tuple{SubArray{Union{Missing, Int}, 1, Vector{Union{Missing, Int}}, Tuple{Vector{Int}}, false}, SubArray{Union{Missing, String}, 1, Vector{Union{Missing, String}}, Tuple{Vector{Int}}, false}}, 2}})
        Base.precompile(Tuple{typeof(_innerjoin_sorted),OnCol{Tuple{Vector{UInt32}, Vector{UInt32}, Vector{UInt32}}, 3},OnCol{Tuple{Vector{UInt32}, Vector{UInt32}, Vector{UInt32}}, 3}})
        Base.precompile(Tuple{typeof(_innerjoin_unsorted_int),SubArray{Union{Missing, Int}, 1, Vector{Union{Missing, Int}}, Tuple{Vector{Int}}, false},Vector{Missing}})
        Base.precompile(Tuple{typeof(_innerjoin_unsorted_int),Vector{Int32},Vector{Int32}})
        Base.precompile(Tuple{typeof(_innerjoin_unsorted),OnCol{Tuple{Vector{Union{Missing, String}}, Vector{Union{Missing, String}}, Vector{Union{Missing, String}}}, 3},OnCol{Tuple{Vector{String}, Vector{String}, Vector{String}}, 3}})
        Base.precompile(Tuple{typeof(_innerjoin_unsorted),OnCol{Tuple{Vector{Union{Missing, Symbol}}, Vector{Union{Missing, Symbol}}}, 2},OnCol{Tuple{Vector{Union{Missing, Symbol}}, Vector{Union{Missing, Symbol}}}, 2}})
        Base.precompile(Tuple{typeof(_mean_fast),Vector{Vector{Union{Missing, Int}}}})
        Base.precompile(Tuple{typeof(_semijoin_unsorted_int),Vector{Int},Vector{Union{Missing, Int}},BitVector,Bool})
        Base.precompile(Tuple{typeof(_semijoin_unsorted),OnCol{Tuple{Vector{UInt32}, Vector{UInt32}}, 2},OnCol{Tuple{Vector{Union{Missing, UInt32}}, Vector{Union{Missing, UInt32}}}, 2},BitVector,Bool})
        Base.precompile(Tuple{typeof(Base.Broadcast.dotview),SubDataFrame{DataFrame, Index, Vector{Int}},Colon,Symbol})
        Base.precompile(Tuple{typeof(describe),DataFrame})
        Base.precompile(Tuple{typeof(do_call),ComposedFunction{typeof(sum), typeof(skipmissing)},Vector{Int},Vector{Int},Vector{Int},GroupedDataFrame{DataFrame},Tuple{Vector{Union{Missing, String}}},Int})
        Base.precompile(Tuple{typeof(do_call),typeof(cor),Vector{Int},Vector{Int},Vector{Int},GroupedDataFrame{DataFrame},Tuple{Vector{Int}, Vector{Int}},Int})
        Base.precompile(Tuple{typeof(do_call),typeof(sum),Vector{Int},Vector{Int},Vector{Int},GroupedDataFrame{DataFrame},Tuple{Vector{DataFrame}},Int})
        Base.precompile(Tuple{typeof(groupby),DataFrame,Symbol})
        Base.precompile(Tuple{typeof(names),DataFrameRow{DataFrame, SubIndex{Index, Vector{Int}, Vector{Int}}},Cols{Tuple{String}}})
        Base.precompile(Tuple{typeof(permutedims),DataFrame,Symbol})
        Base.precompile(Tuple{typeof(prepare_on_col),Vector{Int},Vector{Union{}}})
        Base.precompile(Tuple{typeof(repeat),DataFrame,Int})
        Base.precompile(Tuple{typeof(row_group_slots),Tuple{BitVector},Val{false},Vector{Int},Bool,Nothing})
        Base.precompile(Tuple{typeof(row_group_slots),Tuple{PooledVector{Int, UInt32, Vector{UInt32}}, PooledVector{String, UInt32, Vector{UInt32}}},Val{false},Vector{Int},Bool,Nothing})
        Base.precompile(Tuple{typeof(row_group_slots),Tuple{PooledVector{String, UInt32, Vector{UInt32}}, Vector{Int}},Val{false},Vector{Int},Bool,Nothing})
        Base.precompile(Tuple{typeof(row_group_slots),Tuple{PooledVector{String, UInt32, Vector{UInt32}}, Vector{Int32}},Val{false},Vector{Int},Bool,Nothing})
        Base.precompile(Tuple{typeof(row_group_slots),Tuple{PooledVector{Union{Missing, Int}, UInt32, Vector{UInt32}}, PooledVector{String, UInt32, Vector{UInt32}}},Val{false},Vector{Int},Bool,Nothing})
        Base.precompile(Tuple{typeof(row_group_slots),Tuple{PooledVector{Union{Missing, String}, UInt32, Vector{UInt32}}, PooledVector{String, UInt32, Vector{UInt32}}},Val{false},Vector{Int},Bool,Nothing})
        Base.precompile(Tuple{typeof(row_group_slots),Tuple{RepeatedVector{Union{Missing, Int}}, RepeatedVector{Union{Missing, Int}}, RepeatedVector{Union{Missing, String}}, Vector{Union{Missing, Int}}},Val{false},Vector{Int},Bool,Nothing})
        Base.precompile(Tuple{typeof(row_group_slots),Tuple{SubArray{Int, 1, Vector{Int}, Tuple{Base.OneTo{Int}}, true}},Tuple{IntegerRefpool{Int}},Tuple{IntegerRefarray{SubArray{Int, 1, Vector{Int}, Tuple{Base.OneTo{Int}}, true}}},Val{false},Vector{Int},Bool,Bool})
        Base.precompile(Tuple{typeof(row_group_slots),Tuple{SubArray{Union{Missing, Int}, 1, Vector{Union{Missing, Int}}, Tuple{Base.OneTo{Int}}, true}},Tuple{IntegerRefpool{Union{Missing, Int}}},Tuple{IntegerRefarray{SubArray{Union{Missing, Int}, 1, Vector{Union{Missing, Int}}, Tuple{Base.OneTo{Int}}, true}}},Val{false},Vector{Int},Bool,Bool})
        Base.precompile(Tuple{typeof(row_group_slots),Tuple{Vector{Bool}},Val{false},Vector{Int},Bool,Nothing})
        Base.precompile(Tuple{typeof(row_group_slots),Tuple{Vector{Float64}, Vector{Int}},Val{false},Vector{Int},Bool,Nothing})
        Base.precompile(Tuple{typeof(row_group_slots),Tuple{Vector{Int}, Vector{Int}, Vector{Int}},Val{false},Vector{Int},Bool,Nothing})
        Base.precompile(Tuple{typeof(row_group_slots),Tuple{Vector{Int}, Vector{Union{Missing, Int}}},Val{false},Vector{Int},Bool,Nothing})
        Base.precompile(Tuple{typeof(row_group_slots),Tuple{Vector{Int32}, Vector{Int32}},Val{false},Vector{Int},Bool,Nothing})
        Base.precompile(Tuple{typeof(row_group_slots),Tuple{Vector{Int32}},Tuple{IntegerRefpool{Int}},Tuple{IntegerRefarray{Vector{Int32}}},Val{false},Vector{Int},Bool,Bool})
        Base.precompile(Tuple{typeof(row_group_slots),Tuple{Vector{Int32}},Val{false},Vector{Int},Bool,Nothing})
        Base.precompile(Tuple{typeof(row_group_slots),Tuple{Vector{String}, Vector{Int32}},Val{false},Vector{Int},Bool,Nothing})
        Base.precompile(Tuple{typeof(row_group_slots),Tuple{Vector{Union{Missing, BigInt}}},Val{false},Vector{Int},Bool,Nothing})
        Base.precompile(Tuple{typeof(row_group_slots),Tuple{Vector{Union{Missing, Bool}}},Tuple{IntegerRefpool{Union{Missing, Int}}},Tuple{IntegerRefarray{Vector{Union{Missing, Bool}}}},Val{false},Vector{Int},Bool,Bool})
        Base.precompile(Tuple{typeof(row_group_slots),Tuple{Vector{Union{Missing, Bool}}},Val{false},Vector{Int},Bool,Nothing})
        Base.precompile(Tuple{typeof(row_group_slots),Tuple{Vector{Union{Missing, Float64}}, Vector{Float64}},Val{false},Vector{Int},Bool,Nothing})
        Base.precompile(Tuple{typeof(row_group_slots),Tuple{Vector{Union{Missing, Float64}}, Vector{Int}},Val{false},Vector{Int},Bool,Nothing})
        Base.precompile(Tuple{typeof(row_group_slots),Tuple{Vector{Union{Missing, Int}}, Vector{Union{Missing, Float64}}},Tuple{IntegerRefpool{Union{Missing, Int}}, IntegerRefpool{Union{Missing, Int}}},Tuple{IntegerRefarray{Vector{Union{Missing, Int}}}, IntegerRefarray{Vector{Union{Missing, Float64}}}},Val{false},Vector{Int},Bool,Bool})
        Base.precompile(Tuple{typeof(row_group_slots),Tuple{Vector{Union{Missing, Int}}},Val{false},Vector{Int},Bool,Nothing})
        Base.precompile(Tuple{typeof(select),SubDataFrame{DataFrame, Index, UnitRange{Int}},Any})
        Base.precompile(Tuple{typeof(select),SubDataFrame{DataFrame, SubIndex{Index, UnitRange{Int}, UnitRange{Int}}, Vector{Int}},Any,Any,Vararg{Any}})
        Base.precompile(Tuple{typeof(setindex!),DataFrame,Vector{Int},Vector{Bool},Int})
        Base.precompile(Tuple{typeof(show),IOBuffer,DataFrameRow{DataFrame, SubIndex{Index, UnitRange{Int}, UnitRange{Int}}}})
        Base.precompile(Tuple{typeof(sort),SubDataFrame{DataFrame, SubIndex{Index, UnitRange{Int}, UnitRange{Int}}, UnitRange{Int}}})
        Base.precompile(Tuple{typeof(subset),GroupedDataFrame{SubDataFrame{DataFrame, Index, UnitRange{Int}}},Any})
        Base.precompile(Tuple{typeof(transform),GroupedDataFrame{DataFrame},Union{Regex, AbstractString, Function, Signed, Symbol, Unsigned, Pair, Type, All, Between, Cols, InvertedIndex, AbstractVecOrMat{T} where T}})
        Base.precompile(Tuple{typeof(view),SubDataFrame{DataFrame, Index, UnitRange{Int}},Int,Int})

    if all
        for v in ([1, 2], [2, 1], [2, 2, 1], Int32[1, 2], Int32[2, 1], Int32[2, 2, 1]),
            op in (identity, x -> string.(x), x -> PooledArrays.PooledArray(string.(x))),
            on in (:v1, [:v1, :v2])
            df = DataFrame(v1=op(v), v2=v)
            combine(groupby(df, on), identity, :v1 => identity,
                    :v2 => ByRow(identity), :v2 => sum)
            innerjoin(df, select(df, on), on=on)
            outerjoin(df, select(df, on), on=on)
        end
    end
    return nothing
end
