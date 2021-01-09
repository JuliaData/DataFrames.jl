let m = which(Tuple{typeof(getindex), SubIndex, Union{AbstractVector{AbstractString}, AbstractVector{Symbol}}})
    m.nospecialize |= 1
end
let m = which(Tuple{typeof(iterate), DataFrameRow})
    m.nospecialize |= 1
end
let m = which(Tuple{typeof(disallowmissing), AbstractDataFrame, Union{Colon, Regex, AbstractString, Signed, Symbol, Unsigned, AbstractVector{T} where T, All, Between, Cols, InvertedIndex}})
    m.nospecialize |= 2
end
let m = which(Core.kwfunc(push!), (Any,typeof(push!),DataFrame,DataFrameRow,))
    m.nospecialize |= 9
end
let m = which(Tuple{typeof(getindex), DataFrame, Integer, Union{AbstractString, Symbol}})
    m.nospecialize |= 6
end
let m = which(Core.kwfunc(hcat), (Any,typeof(hcat),Any,AbstractDataFrame,))
    m.nospecialize |= 4
end
let m = which(Core.kwfunc(sort), (Any,typeof(sort),AbstractDataFrame,))
    m.nospecialize |= 1
end
let m = which(Tuple{typeof(wrap_row), Union{AbstractArray{var"#s249", 0} where var"#s249", Ref}, Val{firstmulticol}} where firstmulticol)
    m.nospecialize |= 1
end
let m = which(Tuple{typeof(length), RepeatedVector})
    m.nospecialize |= 1
end
let m = which(Core.kwfunc(hcat!), (Any,typeof(hcat!),AbstractVector{T} where T,DataFrame,))
    m.nospecialize |= 5
end
let m = which(Core.kwfunc(dropmissing), (Any,typeof(dropmissing),AbstractDataFrame,Union{Colon, Regex, AbstractString, Signed, Symbol, Unsigned, AbstractVector{T} where T, All, Between, Cols, InvertedIndex},))
    m.nospecialize |= 5
end
let m = which(Tuple{Type{GroupedDataFrame{T}}, Any, Any, Any, Any, Any, Any, Any, Any, Any} where T<:AbstractDataFrame)
    m.nospecialize |= 185
end
let m = only(methods(Base.bodyfunction(which(repeat!, (DataFrame,)))))
    m.nospecialize |= 3
end
let m = which(Tuple{typeof(iterate), DataFrameColumns})
    m.nospecialize |= 1
end
let m = which(Tuple{typeof(convert), Type{Vector{T} where T}, DataFrameRow})
    m.nospecialize |= 2
end
let m = which(Tuple{typeof(funname), ComposedFunction})
    m.nospecialize |= 1
end
let m = which(Core.kwfunc(hcat!), (Any,typeof(hcat!),DataFrame,AbstractVector{T} where T,))
    m.nospecialize |= 9
end
let m = only(methods(Base.bodyfunction(which(fromcolumns, (Any,Any,)))))
    m.nospecialize |= 4
end
let m = which(Tuple{typeof(ourstrwidth), IO, Any, IOBuffer, Int64})
    m.nospecialize |= 2
end
let m = which(Tuple{typeof(parentcols), SubIndex, Symbol})
    m.nospecialize |= 3
end
let m = which(Tuple{typeof(filter!), Pair{var"#s32", B} where B where var"#s32"<:(AbstractVector{var"#s31"} where var"#s31"<:AbstractString), AbstractDataFrame})
    m.nospecialize |= 1
end
let m = which(Tuple{typeof(filter), Pair{var"#s248", B} where B where var"#s248"<:Union{AbstractString, Signed, Symbol, Unsigned}, GroupedDataFrame})
    m.nospecialize |= 1
end
let m = which(Core.kwfunc(repeat!), (Any,typeof(repeat!),DataFrame,))
    m.nospecialize |= 1
end
let m = which(Tuple{typeof(getproperty), DataFrameRow, Symbol})
    m.nospecialize |= 3
end
let m = which(Tuple{typeof(disallowmissing!), DataFrame, Union{Colon, Regex, AbstractVector{T} where T, All, Between, Cols, InvertedIndex}})
    m.nospecialize |= 2
end
let m = only(methods(Base.bodyfunction(which(hcat, (Any,AbstractDataFrame,)))))
    m.nospecialize |= 8
end
let m = which(Tuple{typeof(Base.Broadcast.broadcast_unalias), Any, AbstractDataFrame})
    m.nospecialize |= 3
end
let m = which(Tuple{typeof(check_aggregate), ComposedFunction{typeof(maximum), typeof(skipmissing)}, AbstractVector{var"#s249"} where var"#s249"<:Union{Missing, Real}})
    m.nospecialize |= 2
end
let m = which(Tuple{typeof(check_aggregate), ComposedFunction{typeof(minimum), typeof(skipmissing)}, AbstractVector{var"#s249"} where var"#s249"<:Union{Missing, Real}})
    m.nospecialize |= 2
end
let m = only(methods(Base.bodyfunction(which(disallowmissing!, (DataFrame,Union{Colon, Regex, AbstractVector{T} where T, All, Between, Cols, InvertedIndex},)))))
    m.nospecialize |= 8
end
let m = which(Tuple{typeof(transform!), DataFrame, Vararg{Any, N} where N})
    m.nospecialize |= 2
end
let m = which(Tuple{typeof(check_aggregate), ComposedFunction{typeof(std), typeof(skipmissing)}, AbstractVector{var"#s249"} where var"#s249"<:Union{Missing, Number}})
    m.nospecialize |= 2
end
let m = which(Tuple{typeof(check_aggregate), typeof(minimum), AbstractVector{var"#s249"} where var"#s249"<:Union{Missing, Real}})
    m.nospecialize |= 2
end
let m = only(methods(Base.bodyfunction(which(hcat!, (DataFrame,Any,Vararg{Any, N} where N,)))))
    m.nospecialize |= 32
end
let m = which(Tuple{typeof(check_aggregate), ComposedFunction{typeof(var), typeof(skipmissing)}, AbstractVector{var"#s249"} where var"#s249"<:Union{Missing, Number}})
    m.nospecialize |= 2
end
let m = which(Tuple{typeof(check_aggregate), ComposedFunction{typeof(prod), typeof(skipmissing)}, AbstractVector{var"#s249"} where var"#s249"<:Union{Missing, Number}})
    m.nospecialize |= 2
end
let m = which(Tuple{Type{Vector{T} where T}, DataFrameRow})
    m.nospecialize |= 1
end
let m = which(Tuple{typeof(check_aggregate), ComposedFunction{typeof(mean), typeof(skipmissing)}, AbstractVector{var"#s249"} where var"#s249"<:Union{Missing, Number}})
    m.nospecialize |= 2
end
let m = which(Core.kwfunc(sort), (Any,typeof(sort),AbstractDataFrame,Any,))
    m.nospecialize |= 9
end
let m = which(Tuple{typeof(rename!), Function, Index})
    m.nospecialize |= 1
end
let m = which(Core.kwfunc(hcat!), (Any,typeof(hcat!),DataFrame,Any,Vararg{Any, N} where N,))
    m.nospecialize |= 17
end
let m = which(Tuple{typeof(check_aggregate), ComposedFunction{typeof(sum), typeof(skipmissing)}, AbstractVector{var"#s249"} where var"#s249"<:Union{Missing, Number}})
    m.nospecialize |= 2
end
let m = which(Core.kwfunc(dropmissing), (Any,typeof(dropmissing),AbstractDataFrame,))
    m.nospecialize |= 5
end
let m = which(Tuple{typeof(getindex), SubDataFrame, Colon, Union{AbstractString, Signed, Symbol, Unsigned}})
    m.nospecialize |= 5
end
let m = which(Tuple{typeof(dropmissing), AbstractDataFrame})
    m.nospecialize |= 1
end
let m = which(Tuple{typeof(check_aggregate), ComposedFunction{typeof(last), typeof(skipmissing)}, AbstractVector{T} where T})
    m.nospecialize |= 2
end
let m = which(Core.kwfunc(Type), (Any,Type{DataFrame},AbstractVector{T} where T,AbstractVector{Symbol},))
    m.nospecialize |= 5
end
let m = which(Tuple{typeof(repeat), AbstractDataFrame, Integer})
    m.nospecialize |= 2
end
let m = which(Tuple{typeof(names), GroupedDataFrame, Any})
    m.nospecialize |= 2
end
let m = which(Tuple{typeof(nonunique), AbstractDataFrame})
    m.nospecialize |= 1
end
let m = which(Tuple{typeof(check_aggregate), typeof(var), AbstractVector{var"#s249"} where var"#s249"<:Union{Missing, Number}})
    m.nospecialize |= 2
end
let m = which(Tuple{typeof(check_aggregate), ComposedFunction{typeof(first), typeof(skipmissing)}, AbstractVector{T} where T})
    m.nospecialize |= 2
end
let m = which(Tuple{typeof(wrap_table), Union{AbstractDataFrame, AbstractMatrix{T} where T, NamedTuple{var"#s249", var"#s248"} where var"#s248"<:Tuple{Vararg{AbstractVector{T} where T, N} where N} where var"#s249"}, Val{firstmulticol}} where firstmulticol)
    m.nospecialize |= 1
end
let m = which(Tuple{typeof(check_aggregate), typeof(std), AbstractVector{var"#s249"} where var"#s249"<:Union{Missing, Number}})
    m.nospecialize |= 2
end
let m = which(Tuple{typeof(isequal_row), Tuple{AbstractVector{T} where T}, Int64, Int64})
    m.nospecialize |= 1
end
let m = which(Tuple{typeof(insert_single_entry!), DataFrame, Any, Integer, Union{AbstractString, Signed, Symbol, Unsigned}})
    m.nospecialize |= 14
end
let m = which(Tuple{Type{SubDataFrame}, DataFrame, AbstractVector{Bool}, Any})
    m.nospecialize |= 4
end
let m = only(methods(Base.bodyfunction(which(DataFrame, (SubDataFrame,)))))
    m.nospecialize |= 4
end
let m = which(Core.kwfunc(Type), (Any,Type{DataFrame},AbstractVector{T} where T,Symbol,))
    m.nospecialize |= 4
end
let m = which(Core.kwfunc(filter), (Any,typeof(filter),Pair,AbstractDataFrame,))
    m.nospecialize |= 4
end
let m = which(Tuple{typeof(convert), Type{DataFrame}, AbstractDict})
    m.nospecialize |= 2
end
let m = only(methods(Base.bodyfunction(which(disallowmissing!, (DataFrame,AbstractVector{var"#s33"} where var"#s33"<:Union{AbstractString, Signed, Symbol, Unsigned},)))))
    m.nospecialize |= 8
end
let m = only(methods(Base.bodyfunction(which(fromcolumns, (Tables.CopiedColumns,Any,)))))
    m.nospecialize |= 4
end
let m = which(Tuple{typeof(check_aggregate), typeof(prod), AbstractVector{var"#s249"} where var"#s249"<:Union{Missing, Number}})
    m.nospecialize |= 2
end
let m = which(Tuple{typeof(do_call), Union{Function, Type}, AbstractVector{var"#s249"} where var"#s249"<:Integer, AbstractVector{var"#s248"} where var"#s248"<:Integer, AbstractVector{var"#s146"} where var"#s146"<:Integer, GroupedDataFrame, NTuple{4, AbstractVector{T} where T}, Integer})
    m.nospecialize |= 1
end
let m = which(Tuple{typeof(check_aggregate), typeof(mean), AbstractVector{var"#s249"} where var"#s249"<:Union{Missing, Number}})
    m.nospecialize |= 2
end
let m = which(Tuple{typeof(_names), DataFrameRow})
    m.nospecialize |= 1
end
let m = which(Tuple{typeof(_pretty_tables_general_formatter), Any, Integer, Integer})
    m.nospecialize |= 1
end
let m = which(Tuple{typeof(check_aggregate), typeof(first), AbstractVector{T} where T})
    m.nospecialize |= 2
end
let m = which(Tuple{typeof(setindex!), DataFrame, Union{AbstractDict, NamedTuple, DataFrameRow}, Integer, Colon})
    m.nospecialize |= 2
end
let m = which(Tuple{typeof(setindex!), DataFrame, Union{Tuple, AbstractArray}, Integer, AbstractVector{T} where T})
    m.nospecialize |= 10
end
let m = which(Tuple{typeof(check_aggregate), typeof(sum), AbstractVector{var"#s249"} where var"#s249"<:Union{Missing, Number}})
    m.nospecialize |= 2
end
let m = which(Tuple{typeof(do_call), Union{Function, Type}, AbstractVector{var"#s249"} where var"#s249"<:Integer, AbstractVector{var"#s248"} where var"#s248"<:Integer, AbstractVector{var"#s146"} where var"#s146"<:Integer, GroupedDataFrame, Tuple{}, Integer})
    m.nospecialize |= 1
end
let m = only(methods(Base.bodyfunction(which(manipulate, (DataFrame,Union{AbstractString, Signed, Symbol, Unsigned},)))))
    m.nospecialize |= 32
end
let m = which(Tuple{typeof(convert), Type{Vector{T}}, DataFrameRow} where T)
    m.nospecialize |= 3
end
let m = which(Tuple{typeof(check_aggregate), typeof(last), AbstractVector{T} where T})
    m.nospecialize |= 2
end
let m = which(Tuple{typeof(filter), Pair, GroupedDataFrame})
    m.nospecialize |= 1
end
let m = only(methods(Base.bodyfunction(which(hcat, (AbstractDataFrame,AbstractDataFrame,)))))
    m.nospecialize |= 8
end
let m = which(Tuple{typeof(Base.Broadcast.broadcast_unalias), AbstractDataFrame, AbstractDataFrame})
    m.nospecialize |= 3
end
let m = which(Core.kwfunc(fromcolumns), (Any,typeof(fromcolumns),Any,Any,))
    m.nospecialize |= 5
end
let m = which(Tuple{typeof(select!), GroupedDataFrame{DataFrame}, Vararg{Any, N} where N})
    m.nospecialize |= 2
end
let m = which(Core.kwfunc(select!), (Any,typeof(select!),DataFrame,Vararg{Any, N} where N,))
    m.nospecialize |= 9
end
let m = which(Tuple{typeof(filter!), Pair, AbstractDataFrame})
    m.nospecialize |= 1
end
let m = which(Tuple{Type{Reduce}, O, C, A, Bool} where A where C where O)
    m.nospecialize |= 9
end
let m = which(Tuple{typeof(similar), RepeatedVector, Type, Tuple{Vararg{Int64, N}} where N})
    m.nospecialize |= 3
end
let m = which(Tuple{typeof(rename!), AbstractDataFrame, Union{AbstractDict{var"#s15", var"#s14"} where var"#s14"<:AbstractString where var"#s15"<:Integer, AbstractDict{var"#s13", Symbol} where var"#s13"<:Integer, AbstractVector{var"#s20"} where var"#s20"<:(Pair{var"#s19", var"#s18"} where var"#s18"<:AbstractString where var"#s19"<:Integer), AbstractVector{var"#s17"} where var"#s17"<:(Pair{var"#s16", Symbol} where var"#s16"<:Integer)}})
    m.nospecialize |= 2
end
let m = which(Tuple{typeof(getindex), SubDataFrame, Integer, Union{AbstractString, Signed, Symbol, Unsigned}})
    m.nospecialize |= 7
end
let m = which(Tuple{typeof(create_bc_tmp), Base.Broadcast.Broadcasted{T, Axes, F, Args} where Args<:Tuple where F where Axes} where T)
    m.nospecialize |= 1
end
let m = only(methods(Base.bodyfunction(which(transform!, (DataFrame,Vararg{Any, N} where N,)))))
    m.nospecialize |= 8
end
let m = which(Tuple{typeof(_dict_to_tuple), AbstractDict{var"#s249", V} where V where var"#s249"<:AbstractString, GroupedDataFrame})
    m.nospecialize |= 1
end
let m = which(Core.kwfunc(combine), (Any,typeof(combine),Union{Function, Type},GroupedDataFrame,))
    m.nospecialize |= 1
end
let m = which(Tuple{Type{DataFrame}, AbstractArray{NamedTuple{names, T}, 1}} where T where names)
    m.nospecialize |= 1
end
let m = which(Tuple{typeof(_nrow), NamedTuple{var"#s249", var"#s248"} where var"#s248"<:Tuple{Vararg{AbstractVector{T} where T, N} where N} where var"#s249"})
    m.nospecialize |= 1
end
let m = which(Tuple{Type{GroupedDataFrame}, T, Vector{Symbol}, Vector{Int64}, Union{Nothing, Vector{Int64}}, Union{Nothing, Vector{Int64}}, Union{Nothing, Vector{Int64}}, Int64, Union{Nothing, Dict{Any, Int64}}, ReentrantLock} where T<:AbstractDataFrame)
    m.nospecialize |= 185
end
let m = which(Tuple{typeof(_add_multicol_res), DataFrameRow, DataFrame, AbstractDataFrame, AbstractVector{Symbol}, Ref{Bool}, Any, Union{Nothing, Int64, AbstractVector{Int64}, AsTable}, Bool, Union{Nothing, AbstractVector{Symbol}, Type{AsTable}}})
    m.nospecialize |= 356
end
let m = which(Tuple{Type{SubDataFrame}, DataFrame, AbstractVector{T} where T, Any})
    m.nospecialize |= 6
end
let m = which(Tuple{typeof(check_aggregate), typeof(length), AbstractVector{T} where T})
    m.nospecialize |= 2
end
let m = which(Tuple{typeof(==), DataFrameRow, DataFrameRow})
    m.nospecialize |= 3
end
let m = which(Tuple{typeof(rename!), AbstractDataFrame, Union{AbstractDict{Symbol, Symbol}, AbstractDict{Symbol, var"#s15"} where var"#s15"<:AbstractString, AbstractDict{var"#s16", Symbol} where var"#s16"<:AbstractString, AbstractDict{var"#s17", var"#s18"} where var"#s18"<:AbstractString where var"#s17"<:AbstractString, AbstractVector{var"#s12"} where var"#s12"<:(Pair{Symbol, var"#s6"} where var"#s6"<:AbstractString), AbstractVector{var"#s4"} where var"#s4"<:(Pair{var"#s3", Symbol} where var"#s3"<:AbstractString), AbstractVector{var"#s2"} where var"#s2"<:(Pair{var"#s13", var"#s14"} where var"#s14"<:AbstractString where var"#s13"<:AbstractString)}})
    m.nospecialize |= 2
end
let m = which(Tuple{typeof(wrap), NamedTuple{var"#s249", var"#s248"} where var"#s248"<:Tuple{Vararg{AbstractVector{T} where T, N} where N} where var"#s249"})
    m.nospecialize |= 1
end
let m = which(Core.kwfunc(Type), (Any,Type{DataFrame},SubDataFrame,))
    m.nospecialize |= 4
end
let m = which(Core.kwfunc(select!), (Any,typeof(select!),GroupedDataFrame{DataFrame},Vararg{Any, N} where N,))
    m.nospecialize |= 9
end
let m = which(Tuple{typeof(getindex), SubDataFrame, Union{AbstractVector{T} where T, InvertedIndex}, Union{AbstractString, Signed, Symbol, Unsigned}})
    m.nospecialize |= 7
end
let m = which(Tuple{typeof(isequal), AbstractIndex, AbstractIndex})
    m.nospecialize |= 3
end
let m = only(methods(Base.bodyfunction(which(insertcols!, (DataFrame,Union{AbstractString, Signed, Symbol, Unsigned},Vararg{Pair{var"#s33", var"#s32"} where var"#s32" where var"#s33"<:AbstractString, N} where N,)))))
    m.nospecialize |= 32
end
let m = which(Tuple{typeof(insertcols!), DataFrame, Vararg{Pair{Symbol, var"#s32"} where var"#s32", N} where N})
    m.nospecialize |= 2
end
let m = which(Core.kwfunc(fromcolumns), (Any,typeof(fromcolumns),Tables.CopiedColumns,Any,))
    m.nospecialize |= 5
end
let m = only(methods(Base.bodyfunction(which(_stackview, (AbstractDataFrame,AbstractVector{Int64},AbstractVector{Int64},)))))
    m.nospecialize |= 4
end
let m = which(Tuple{typeof(names), DataFrameRow, Any})
    m.nospecialize |= 2
end
let m = which(Tuple{typeof(hashrows_col!), Vector{UInt64}, Vector{Bool}, AbstractVector{T}, Nothing, Bool} where T)
    m.nospecialize |= 4
end
let m = which(Core.kwfunc(transform), (Any,typeof(transform),AbstractDataFrame,Vararg{Any, N} where N,))
    m.nospecialize |= 13
end
let m = which(Tuple{typeof(setproperty!), DataFrame, Symbol, AbstractVector{T} where T})
    m.nospecialize |= 6
end
let m = which(Tuple{typeof(getindex), RepeatedVector, Int64})
    m.nospecialize |= 1
end
let m = which(Core.kwfunc(manipulate), (Any,typeof(manipulate),DataFrame,Union{AbstractString, Signed, Symbol, Unsigned},))
    m.nospecialize |= 9
end
let m = only(methods(Base.bodyfunction(which(DataFrame, (AbstractVector{<:NamedTuple},)))))
    m.nospecialize |= 4
end
let m = which(Tuple{typeof(getindex), AbstractIndex, AbstractRange{Int64}})
    m.nospecialize |= 3
end
let m = which(Tuple{typeof(wrap_row), NamedTuple, Val{firstmulticol}} where firstmulticol)
    m.nospecialize |= 1
end
let m = only(methods(Base.bodyfunction(which(DataFrame, (AbstractMatrix{T} where T,AbstractVector{Symbol},)))))
    m.nospecialize |= 4
end
let m = which(Tuple{typeof(setindex!), DataFrame, Any, Integer, Union{AbstractString, Signed, Symbol, Unsigned}})
    m.nospecialize |= 14
end
let m = only(methods(Base.bodyfunction(which(antijoin, (AbstractDataFrame,AbstractDataFrame,)))))
    m.nospecialize |= 1
end
let m = which(Tuple{typeof(filter), Pair{var"#s248", B} where B where var"#s248"<:AbstractVector{Int64}, GroupedDataFrame})
    m.nospecialize |= 1
end
let m = only(methods(Base.bodyfunction(which(semijoin, (AbstractDataFrame,AbstractDataFrame,)))))
    m.nospecialize |= 1
end
let m = which(Core.kwfunc(antijoin), (Any,typeof(antijoin),AbstractDataFrame,AbstractDataFrame,))
    m.nospecialize |= 1
end
let m = which(Tuple{typeof(copyto!), AbstractDataFrame, Base.Broadcast.Broadcasted{var"#s249", Axes, F, Args} where Args<:Tuple where F where Axes where var"#s249"<:Base.Broadcast.AbstractArrayStyle{0}})
    m.nospecialize |= 3
end
let m = which(Tuple{typeof(insertcols!), DataFrame, Union{AbstractString, Signed, Symbol, Unsigned}, Vararg{Pair{Symbol, var"#s15"} where var"#s15", N} where N})
    m.nospecialize |= 6
end
let m = which(Tuple{typeof(setindex!), DataFrame, AbstractVector{T} where T, Colon, Union{AbstractString, Signed, Symbol, Unsigned}})
    m.nospecialize |= 10
end
let m = which(Core.kwfunc(semijoin), (Any,typeof(semijoin),AbstractDataFrame,AbstractDataFrame,))
    m.nospecialize |= 1
end
let m = which(Tuple{typeof(view), DataFrameRow, Union{Colon, Regex, AbstractVector{T} where T, All, Between, Cols, InvertedIndex}})
    m.nospecialize |= 3
end
let m = which(Tuple{typeof(getindex), DataFrame, AbstractVector{T}, Colon} where T)
    m.nospecialize |= 2
end
let m = only(methods(Base.bodyfunction(which(append!, (DataFrame,Any,)))))
    m.nospecialize |= 16
end
let m = which(Tuple{typeof(ordering), AbstractDataFrame, Function, Function, Bool, Ordering})
    m.nospecialize |= 1
end
let m = which(Tuple{typeof(convert), Type{Matrix{T}}, AbstractDataFrame} where T)
    m.nospecialize |= 3
end
let m = which(Tuple{Type{DataFrame}, AbstractDict})
    m.nospecialize |= 1
end
let m = which(Tuple{Type{SubDataFrame}, DataFrame, AbstractVector{var"#s146"} where var"#s146"<:Integer, Any})
    m.nospecialize |= 6
end
let m = which(Tuple{Type{Matrix{T} where T}, AbstractDataFrame})
    m.nospecialize |= 1
end
let m = which(Tuple{typeof(_ncol), Union{NamedTuple, DataFrameRow}})
    m.nospecialize |= 1
end
let m = which(Tuple{Type{SubIndex}, I, S, T} where T<:AbstractVector{Int64} where S<:AbstractVector{Int64} where I<:AbstractIndex)
    m.nospecialize |= 6
end
let m = which(Tuple{typeof(copyto!), DataFrameRow, Base.Broadcast.Broadcasted})
    m.nospecialize |= 3
end
let m = which(Tuple{typeof(show), IO, MIME{Symbol("text/latex")}, AbstractDataFrame})
    m.nospecialize |= 4
end
let m = only(methods(Base.bodyfunction(which(transform!, (GroupedDataFrame{DataFrame},Vararg{Any, N} where N,)))))
    m.nospecialize |= 16
end
let m = which(Tuple{typeof(findrow), RowGroupDict, AbstractDataFrame, Tuple{Vararg{AbstractVector{T} where T, N} where N}, Tuple{Vararg{AbstractVector{T} where T, N} where N}, Int64})
    m.nospecialize |= 12
end
let m = which(Tuple{typeof(_add_multicol_res), AbstractMatrix{T} where T, DataFrame, AbstractDataFrame, AbstractVector{Symbol}, Ref{Bool}, Any, Union{Nothing, Int64, AbstractVector{Int64}, AsTable}, Bool, Union{Nothing, AbstractVector{Symbol}, Type{AsTable}}})
    m.nospecialize |= 357
end
let m = which(Tuple{typeof(genkeymap), Any, Any})
    m.nospecialize |= 2
end
let m = which(Tuple{typeof(rename!), AbstractDataFrame, Vararg{Pair, N} where N})
    m.nospecialize |= 3
end
let m = which(Tuple{Type{DataFrame}, AbstractVector{var"#s6"} where var"#s6"<:Pair})
    m.nospecialize |= 1
end
let m = only(methods(Base.bodyfunction(which(repeat, (AbstractDataFrame,)))))
    m.nospecialize |= 11
end
let m = which(Tuple{typeof(do_call), Union{Function, Type}, AbstractVector{var"#s249"} where var"#s249"<:Integer, AbstractVector{var"#s248"} where var"#s248"<:Integer, AbstractVector{var"#s146"} where var"#s146"<:Integer, GroupedDataFrame, Tuple{AbstractVector{T} where T, AbstractVector{T} where T, AbstractVector{T} where T}, Integer})
    m.nospecialize |= 1
end
let m = which(Core.kwfunc(unstack), (Any,typeof(unstack),AbstractDataFrame,Union{AbstractString, Signed, Symbol, Unsigned},Union{AbstractString, Signed, Symbol, Unsigned},))
    m.nospecialize |= 25
end
let m = which(Tuple{typeof(_filter_helper_astable), GroupedDataFrame, NamedTuple, Any, Vector{Int64}, Vector{Int64}, Vector{Int64}})
    m.nospecialize |= 6
end
let m = which(Tuple{typeof(_dict_to_tuple), AbstractDict{Symbol, V} where V, GroupedDataFrame})
    m.nospecialize |= 1
end
let m = which(Tuple{typeof(Base.Sort.defalg), AbstractDataFrame, Any, Ordering})
    m.nospecialize |= 7
end
let m = which(Tuple{typeof(_broadcast_unalias_helper), AbstractDataFrame, AbstractVector{T} where T, AbstractDataFrame, Int64, Bool})
    m.nospecialize |= 7
end
let m = which(Tuple{typeof(to_indices), GroupedDataFrame, Tuple{var"#s248"} where var"#s248"<:InvertedIndex})
    m.nospecialize |= 2
end
let m = only(methods(Base.bodyfunction(which(unstack, (AbstractDataFrame,Union{AbstractString, Signed, Symbol, Unsigned},Union{AbstractString, Signed, Symbol, Unsigned},)))))
    m.nospecialize |= 97
end
let m = which(Tuple{Type{DataFrame}, AbstractVector{var"#s32"} where var"#s32"<:(AbstractVector{T} where T), AbstractVector{Symbol}})
    m.nospecialize |= 1
end
let m = which(Tuple{typeof(_filter_helper_astable), Any, Tables.NamedTupleIterator})
    m.nospecialize |= 3
end
let m = only(methods(Base.bodyfunction(which(select!, (GroupedDataFrame{DataFrame},Vararg{Any, N} where N,)))))
    m.nospecialize |= 16
end
let m = which(Tuple{typeof(getindex), DataFrameRow, Union{Colon, Regex, AbstractVector{T} where T, All, Between, Cols, InvertedIndex}})
    m.nospecialize |= 3
end
let m = which(Tuple{typeof(reduce), typeof(vcat), Union{Tuple{AbstractDataFrame, Vararg{AbstractDataFrame, N} where N}, AbstractVector{var"#s19"} where var"#s19"<:AbstractDataFrame}})
    m.nospecialize |= 2
end
let m = only(methods(Base.bodyfunction(which(show, (IO,GroupedDataFrame,)))))
    m.nospecialize |= 256
end
let m = only(methods(Base.bodyfunction(which(DataFrame, (AbstractVector{var"#s32"} where var"#s32"<:Pair,)))))
    m.nospecialize |= 8
end
let m = which(Tuple{typeof(getindex), DataFrame, InvertedIndex, Union{Colon, Regex, AbstractVector{T} where T, All, Between, Cols, InvertedIndex}})
    m.nospecialize |= 6
end
let m = which(Core.kwfunc(repeat), (Any,typeof(repeat),AbstractDataFrame,))
    m.nospecialize |= 5
end
let m = which(Tuple{typeof(_rename_cols), AbstractVector{Symbol}, Union{AbstractString, Function, Symbol}})
    m.nospecialize |= 2
end
let m = which(Tuple{typeof(Base.to_index), GroupedDataFrame, NamedTuple{N, T} where T<:Tuple} where N)
    m.nospecialize |= 2
end
let m = which(Tuple{typeof(copyto!), ColReplaceDataFrame, Base.Broadcast.Broadcasted})
    m.nospecialize |= 2
end
let m = which(Tuple{typeof(groupreduce_init), typeof(min), Any, Any, AbstractVector{T}, GroupedDataFrame} where T)
    m.nospecialize |= 10
end
let m = which(Tuple{typeof(getindex), AbstractDataFrame, Integer, Colon})
    m.nospecialize |= 3
end
let m = which(Tuple{typeof(completecases), AbstractDataFrame, Union{Colon, Regex, AbstractVector{T} where T, All, Between, Cols, InvertedIndex}})
    m.nospecialize |= 2
end
let m = which(Tuple{typeof(Base.Broadcast.broadcast_unalias), AbstractDataFrame, Any})
    m.nospecialize |= 3
end
let m = which(Tuple{typeof(nonunique), AbstractDataFrame, Any})
    m.nospecialize |= 3
end
let m = only(methods(Base.bodyfunction(which(insertcols!, (DataFrame,Vararg{Pair{Symbol, var"#s33"} where var"#s33", N} where N,)))))
    m.nospecialize |= 16
end
let m = which(Tuple{Type{ByRow{T}}, Any} where T)
    m.nospecialize |= 1
end
let m = which(Tuple{typeof(transform), AbstractDataFrame, Vararg{Any, N} where N})
    m.nospecialize |= 3
end
let m = which(Tuple{Aggregate{typeof(std), C} where C, AbstractVector{T} where T, GroupedDataFrame})
    m.nospecialize |= 1
end
let m = only(methods(Base.bodyfunction(which(ourshow, (IO,Any,Int64,)))))
    m.nospecialize |= 8
end
let m = which(Tuple{Aggregate{typeof(last), C} where C, AbstractVector{T} where T, GroupedDataFrame})
    m.nospecialize |= 1
end
let m = which(Tuple{typeof(allowmissing), AbstractDataFrame, Union{Colon, Regex, AbstractString, Signed, Symbol, Unsigned, AbstractVector{T} where T, All, Between, Cols, InvertedIndex}})
    m.nospecialize |= 2
end
let m = which(Tuple{typeof(Tables.schema), AbstractDataFrame})
    m.nospecialize |= 1
end
let m = which(Tuple{typeof(_add_multicol_res), AbstractDataFrame, DataFrame, AbstractDataFrame, AbstractVector{Symbol}, Ref{Bool}, Any, Union{Nothing, Int64, AbstractVector{Int64}, AsTable}, Bool, Union{Nothing, AbstractVector{Symbol}, Type{AsTable}}})
    m.nospecialize |= 356
end
let m = which(Tuple{Type{DataFrame}, AbstractVector{T} where T, Symbol})
    m.nospecialize |= 1
end
let m = only(methods(Base.bodyfunction(which(_show, (IO,MIME{Symbol("text/latex")},AbstractDataFrame,)))))
    m.nospecialize |= 40
end
let m = which(Core.kwfunc(Type), (Any,Type{DataFrame},AbstractDict,))
    m.nospecialize |= 4
end
let m = which(Tuple{Type{DataFrameJoiner}, AbstractDataFrame, AbstractDataFrame, AbstractVector{T} where T, Symbol})
    m.nospecialize |= 5
end
let m = which(Tuple{typeof(getindex), AbstractIndex, AbstractVector{T} where T})
    m.nospecialize |= 3
end
let m = which(Tuple{typeof(select!), DataFrame, Vararg{Any, N} where N})
    m.nospecialize |= 2
end
let m = only(methods(Base.bodyfunction(which(show, (IO,MIME{Symbol("text/latex")},AbstractDataFrame,)))))
    m.nospecialize |= 16
end
let m = which(Tuple{typeof(_pretty_tables_float_formatter), Any, Integer, Integer, Vector{Int64}, Vector{Int64}, Bool})
    m.nospecialize |= 1
end
let m = which(Tuple{typeof(_add_multicol_res), NamedTuple, DataFrame, AbstractDataFrame, AbstractVector{Symbol}, Ref{Bool}, Any, Union{Nothing, Int64, AbstractVector{Int64}, AsTable}, Bool, Union{Nothing, AbstractVector{Symbol}, Type{AsTable}}})
    m.nospecialize |= 357
end
let m = which(Tuple{var"#ft_float#557", Any, Any, Any})
    m.nospecialize |= 1
end
let m = which(Tuple{Type{DataFrame}, Vararg{Pair{Symbol, var"#s15"} where var"#s15", N} where N})
    m.nospecialize |= 1
end
let m = which(Core.kwfunc(stack), (Any,typeof(stack),AbstractDataFrame,Any,))
    m.nospecialize |= 9
end
let m = which(Tuple{typeof(setindex!), DataFrame, AbstractVector{T} where T, AbstractVector{T} where T, Union{AbstractString, Signed, Symbol, Unsigned}})
    m.nospecialize |= 14
end
let m = only(methods(Base.bodyfunction(which(DataFrame, (Vararg{Pair{var"#s32", var"#s31"} where var"#s31" where var"#s32"<:AbstractString, N} where N,)))))
    m.nospecialize |= 8
end
let m = which(Tuple{typeof(getindex), DataFrame, typeof(!), Union{Signed, Unsigned}})
    m.nospecialize |= 4
end
let m = which(Tuple{typeof(_insert_row_multicolumn), DataFrame, AbstractDataFrame, Ref{Bool}, AbstractVector{Symbol}, Union{NamedTuple, DataFrameRow}})
    m.nospecialize |= 18
end
let m = only(methods(Base.bodyfunction(which(select!, (DataFrame,Vararg{Any, N} where N,)))))
    m.nospecialize |= 8
end
let m = only(methods(Base.bodyfunction(which(innerjoin, (AbstractDataFrame,AbstractDataFrame,Vararg{AbstractDataFrame, N} where N,)))))
    m.nospecialize |= 1
end
let m = which(Core.kwfunc(manipulate), (Any,typeof(manipulate),DataFrame,AbstractVector{Int64},))
    m.nospecialize |= 9
end
let m = which(Core.kwfunc(disallowmissing), (Any,typeof(disallowmissing),AbstractDataFrame,Union{Colon, Regex, AbstractString, Signed, Symbol, Unsigned, AbstractVector{T} where T, All, Between, Cols, InvertedIndex},))
    m.nospecialize |= 8
end
let m = which(Tuple{typeof(dropmissing), AbstractDataFrame, Union{Colon, Regex, AbstractString, Signed, Symbol, Unsigned, AbstractVector{T} where T, All, Between, Cols, InvertedIndex}})
    m.nospecialize |= 3
end
let m = which(Tuple{Type{ByRow}, T} where T)
    m.nospecialize |= 1
end
let m = which(Tuple{typeof(unique), AbstractDataFrame, Any})
    m.nospecialize |= 2
end
let m = only(methods(Base.bodyfunction(which(disallowmissing, (AbstractDataFrame,Union{Colon, Regex, AbstractString, Signed, Symbol, Unsigned, AbstractVector{T} where T, All, Between, Cols, InvertedIndex},)))))
    m.nospecialize |= 8
end
let m = which(Tuple{typeof(names), AbstractDataFrame, Any})
    m.nospecialize |= 3
end
let m = which(Core.kwfunc(_stackview), (Any,typeof(_stackview),AbstractDataFrame,AbstractVector{Int64},AbstractVector{Int64},))
    m.nospecialize |= 1
end
let m = which(Tuple{typeof(view), AbstractDataFrame, typeof(!), Union{Colon, Regex, AbstractVector{T} where T, All, Between, Cols, InvertedIndex}})
    m.nospecialize |= 5
end
let m = which(Tuple{typeof(filter), Pair{var"#s13", B} where B where var"#s13"<:Union{AbstractVector{var"#s12"} where var"#s12"<:Integer, AbstractVector{var"#s6"} where var"#s6"<:AbstractString, AbstractVector{var"#s4"} where var"#s4"<:Symbol}, AbstractDataFrame})
    m.nospecialize |= 1
end
let m = which(Tuple{typeof(findrows), RowGroupDict, AbstractDataFrame, Tuple{Vararg{AbstractVector{T} where T, N} where N}, Tuple{Vararg{AbstractVector{T} where T, N} where N}, Int64})
    m.nospecialize |= 12
end
let m = only(methods(Base.bodyfunction(which(Sort.defalg, (AbstractDataFrame,Ordering,)))))
    m.nospecialize |= 26
end
let m = which(Tuple{typeof(stack), AbstractDataFrame, Any})
    m.nospecialize |= 2
end
let m = only(methods(Base.bodyfunction(which(DataFrame, (AbstractDict,)))))
    m.nospecialize |= 4
end
let m = which(Core.kwfunc(innerjoin), (Any,typeof(innerjoin),AbstractDataFrame,AbstractDataFrame,Vararg{AbstractDataFrame, N} where N,))
    m.nospecialize |= 1
end
let m = which(Tuple{typeof(wrap), Any})
    m.nospecialize |= 1
end
let m = which(Core.kwfunc(transform), (Any,typeof(transform),GroupedDataFrame,Vararg{Any, N} where N,))
    m.nospecialize |= 9
end
let m = which(Tuple{Type{DFPerm}, Union{Ordering, AbstractVector{T} where T}, AbstractDataFrame})
    m.nospecialize |= 3
end
let m = which(Tuple{typeof(Base.to_index), GroupedDataFrame, Union{AbstractDict{Symbol, V} where V, AbstractDict{var"#s249", V} where V where var"#s249"<:AbstractString}})
    m.nospecialize |= 2
end
let m = which(Core.kwfunc(Type), (Any,Type{DataFrame},AbstractMatrix{T} where T,AbstractVector{Symbol},))
    m.nospecialize |= 4
end
let m = which(Tuple{typeof(getindex), AbstractIndex, Cols})
    m.nospecialize |= 3
end
let m = which(Core.kwfunc(stack), (Any,typeof(stack),AbstractDataFrame,Any,Any,))
    m.nospecialize |= 25
end
let m = only(methods(Base.bodyfunction(which(DataFrame, (Vararg{Pair{Symbol, var"#s32"} where var"#s32", N} where N,)))))
    m.nospecialize |= 8
end
let m = only(methods(Base.bodyfunction(which(unique, (AbstractDataFrame,Any,)))))
    m.nospecialize |= 8
end
let m = which(Tuple{typeof(funname), ByRow})
    m.nospecialize |= 1
end
let m = which(Tuple{typeof(transform), GroupedDataFrame, Vararg{Any, N} where N})
    m.nospecialize |= 2
end
let m = which(Tuple{typeof(rename), AbstractDataFrame, Vararg{Any, N} where N})
    m.nospecialize |= 2
end
let m = which(Tuple{typeof(row_group_slots), Tuple{Vararg{AbstractVector{T} where T, N} where N}, Val})
    m.nospecialize |= 1
end
let m = which(Tuple{Type{DFPerm}, O, T} where T<:Tuple where O<:Union{Ordering, AbstractVector{T} where T})
    m.nospecialize |= 3
end
let m = which(Tuple{typeof(_empty_astable_helper), Any, Any})
    m.nospecialize |= 1
end
let m = which(Tuple{Type{DFPerm{O, T}}, Any, Any} where T<:Tuple where O<:Union{Ordering, AbstractVector{T} where T})
    m.nospecialize |= 3
end
let m = which(Tuple{Type{DataFrameRow}, D, S, Union{Signed, Unsigned}, Union{Signed, Unsigned}} where S<:AbstractIndex where D<:AbstractDataFrame)
    m.nospecialize |= 14
end
let m = which(Core.kwfunc(_show), (Any,typeof(_show),IO,MIME{Symbol("text/latex")},AbstractDataFrame,))
    m.nospecialize |= 21
end
let m = which(Tuple{typeof(==), AbstractDataFrame, AbstractDataFrame})
    m.nospecialize |= 3
end
let m = which(Tuple{typeof(getindex), DataFrame, Integer, Union{Signed, Unsigned}})
    m.nospecialize |= 6
end
let m = which(Tuple{typeof(copy), DataFrameRow})
    m.nospecialize |= 1
end
let m = which(Tuple{typeof(getindex), DataFrame, typeof(!), Union{Colon, Regex, AbstractVector{T} where T, All, Between, Cols, InvertedIndex}})
    m.nospecialize |= 4
end
let m = only(methods(Base.bodyfunction(which(DataFrame, (AbstractVector{T} where T,Symbol,)))))
    m.nospecialize |= 4
end
let m = which(Tuple{typeof(rowhash), Tuple{Vararg{AbstractVector{T} where T, N} where N}, Int64, UInt64})
    m.nospecialize |= 1
end
let m = which(Tuple{typeof(_empty_selector_helper), Any, Any})
    m.nospecialize |= 1
end
let m = which(Tuple{typeof(_add_multicol_res), NamedTuple{var"#s249", var"#s248"} where var"#s248"<:Tuple{Vararg{AbstractVector{T} where T, N} where N} where var"#s249", DataFrame, AbstractDataFrame, AbstractVector{Symbol}, Ref{Bool}, Any, Union{Nothing, Int64, AbstractVector{Int64}, AsTable}, Bool, Union{Nothing, AbstractVector{Symbol}, Type{AsTable}}})
    m.nospecialize |= 357
end
let m = which(Tuple{typeof(push!), DataFrame, Any})
    m.nospecialize |= 2
end
let m = which(Tuple{Type{SubDataFrame}, SubDataFrame, Any, Colon})
    m.nospecialize |= 3
end
let m = which(Tuple{typeof(parentcols), SubIndex, Regex})
    m.nospecialize |= 3
end
let m = which(Tuple{typeof(_rename_cols), AbstractVector{Symbol}, Union{AbstractString, Function, Symbol}, AbstractVector{Symbol}})
    m.nospecialize |= 3
end
let m = which(Tuple{Type{NamedTuple}, DataFrameRow})
    m.nospecialize |= 1
end
let m = only(methods(Base.bodyfunction(which(dropmissing, (AbstractDataFrame,Union{Colon, Regex, AbstractString, Signed, Symbol, Unsigned, AbstractVector{T} where T, All, Between, Cols, InvertedIndex},)))))
    m.nospecialize |= 27
end
let m = only(methods(Base.bodyfunction(which(filter, (Pair{var"#s18", B} where B where var"#s18"<:Union{AbstractVector{var"#s17"} where var"#s17"<:Integer, AbstractVector{var"#s16"} where var"#s16"<:AbstractString, AbstractVector{var"#s15"} where var"#s15"<:Symbol},AbstractDataFrame,)))))
    m.nospecialize |= 4
end
let m = which(Tuple{typeof(isequal_row), Tuple{Vararg{AbstractVector{T} where T, N} where N}, Int64, Int64})
    m.nospecialize |= 1
end
let m = which(Core.kwfunc(insertcols!), (Any,typeof(insertcols!),DataFrame,Union{AbstractString, Signed, Symbol, Unsigned},Vararg{Pair{Symbol, var"#s14"} where var"#s14", N} where N,))
    m.nospecialize |= 17
end
let m = which(Tuple{typeof(ordering), AbstractDataFrame, AbstractVector{T} where T, Function, Function, Bool, Ordering})
    m.nospecialize |= 3
end
let m = which(Tuple{Type{DataFrame}, AbstractMatrix{T} where T, Symbol})
    m.nospecialize |= 1
end
let m = only(methods(Base.bodyfunction(which(DataFrame, (AbstractVector{var"#s33"} where var"#s33"<:(AbstractVector{T} where T),AbstractVector{Symbol},)))))
    m.nospecialize |= 24
end
let m = which(Tuple{typeof(getindex), DataFrameColumns, Union{Colon, Regex, AbstractVector{T} where T, All, Between, Cols, InvertedIndex}})
    m.nospecialize |= 3
end
let m = which(Tuple{typeof(getindex), AbstractDataFrame, Integer, Union{Colon, Regex, AbstractVector{T} where T, All, Between, Cols, InvertedIndex}})
    m.nospecialize |= 7
end
let m = which(Tuple{typeof(filter), Pair, AbstractDataFrame})
    m.nospecialize |= 1
end
let m = which(Core.kwfunc(issorted), (Any,typeof(issorted),AbstractDataFrame,))
    m.nospecialize |= 1
end
let m = which(Tuple{typeof(Base.to_index), GroupedDataFrame, AbstractVector{T}} where T)
    m.nospecialize |= 2
end
let m = which(Tuple{typeof(push!), DataFrame, Union{AbstractDict, NamedTuple}})
    m.nospecialize |= 2
end
let m = which(Tuple{typeof(getindex), AbstractIndex, Integer})
    m.nospecialize |= 3
end
let m = which(Tuple{Aggregate{typeof(first), C} where C, AbstractVector{T} where T, GroupedDataFrame})
    m.nospecialize |= 1
end
let m = which(Core.kwfunc(issorted), (Any,typeof(issorted),AbstractDataFrame,Any,))
    m.nospecialize |= 1
end
let m = which(Tuple{typeof(get_stats), AbstractVector{T} where T, AbstractVector{Symbol}})
    m.nospecialize |= 1
end
let m = only(methods(Base.bodyfunction(which(transform, (AbstractDataFrame,Vararg{Any, N} where N,)))))
    m.nospecialize |= 27
end
let m = which(Tuple{typeof(select), AbstractDataFrame, Vararg{Any, N} where N})
    m.nospecialize |= 3
end
let m = which(Tuple{typeof(groupreduce_init), typeof(max), Any, Any, AbstractVector{T}, GroupedDataFrame} where T)
    m.nospecialize |= 10
end
let m = which(Tuple{typeof(view), AbstractDataFrame, InvertedIndex, Union{Colon, Regex, AbstractVector{T} where T, All, Between, Cols, InvertedIndex}})
    m.nospecialize |= 7
end
let m = which(Tuple{typeof(do_call), Union{Function, Type}, AbstractVector{var"#s249"} where var"#s249"<:Integer, AbstractVector{var"#s248"} where var"#s248"<:Integer, AbstractVector{var"#s146"} where var"#s146"<:Integer, GroupedDataFrame, NamedTuple, Integer})
    m.nospecialize |= 33
end
let m = which(Tuple{typeof(_transformation_helper), AbstractDataFrame, AsTable, Any})
    m.nospecialize |= 4
end
let m = which(Core.kwfunc(show), (Any,typeof(show),IO,AbstractDataFrame,))
    m.nospecialize |= 5
end
let m = which(Tuple{Type{DataFrame}, T} where T)
    m.nospecialize |= 1
end
let m = which(Tuple{typeof(getindex), SubDataFrame, Union{AbstractVector{T} where T, InvertedIndex}, Union{Colon, Regex, AbstractVector{T} where T, All, Between, Cols, InvertedIndex}})
    m.nospecialize |= 7
end
let m = which(Core.kwfunc(unstack), (Any,typeof(unstack),AbstractDataFrame,Any,Union{AbstractString, Signed, Symbol, Unsigned},Union{AbstractString, Signed, Symbol, Unsigned},))
    m.nospecialize |= 57
end
let m = which(Tuple{typeof(getindex), DataFrameRow, Union{AbstractString, Signed, Symbol, Unsigned}})
    m.nospecialize |= 3
end
let m = which(Tuple{typeof(stack), AbstractDataFrame, Any, Any})
    m.nospecialize |= 6
end
let m = which(Tuple{typeof(setindex!), SubDataFrame, Any, Any, Any})
    m.nospecialize |= 15
end
let m = only(methods(Base.bodyfunction(which(transform, (GroupedDataFrame,Vararg{Any, N} where N,)))))
    m.nospecialize |= 64
end
let m = which(Tuple{typeof(view), AbstractDataFrame, Integer, Union{Colon, Regex, AbstractVector{T} where T, All, Between, Cols, InvertedIndex}})
    m.nospecialize |= 7
end
let m = only(methods(Base.bodyfunction(which(show, (IO,AbstractDataFrame,)))))
    m.nospecialize |= 768
end
let m = which(Tuple{typeof(_copyto_helper!), AbstractVector{T} where T, Base.Broadcast.Broadcasted, Int64})
    m.nospecialize |= 3
end
let m = which(Tuple{typeof(setindex!), DataFrameRow, Any, Any})
    m.nospecialize |= 7
end
let m = only(methods(Base.bodyfunction(which(filter, (Pair,AbstractDataFrame,)))))
    m.nospecialize |= 4
end
let m = which(Tuple{Aggregate{typeof(length), C} where C, AbstractVector{T} where T, GroupedDataFrame})
    m.nospecialize |= 1
end
let m = which(Tuple{typeof(groupby), AbstractDataFrame, Any})
    m.nospecialize |= 3
end
let m = which(Tuple{typeof(setindex!), DataFrame, AbstractVector{T} where T, typeof(!), Union{AbstractString, Signed, Symbol, Unsigned}})
    m.nospecialize |= 10
end
let m = which(Tuple{Aggregate{typeof(var), C} where C, AbstractVector{T} where T, GroupedDataFrame})
    m.nospecialize |= 1
end
let m = which(Core.kwfunc(fillfirst!), (Any,typeof(fillfirst!),Any,AbstractVector{T} where T,AbstractVector{T} where T,GroupedDataFrame,))
    m.nospecialize |= 25
end
let m = which(Tuple{Type{DataFrameRow}, SubDataFrame, Integer, Any})
    m.nospecialize |= 7
end
let m = which(Tuple{typeof(copyto_widen!), AbstractVector{T}, AbstractVector{T} where T} where T)
    m.nospecialize |= 3
end
let m = only(methods(Base.bodyfunction(which(fillfirst!, (Any,AbstractVector{T} where T,AbstractVector{T} where T,GroupedDataFrame,)))))
    m.nospecialize |= 60
end
let m = which(Tuple{typeof(_expand_to_table), AbstractVector{T} where T})
    m.nospecialize |= 1
end
let m = which(Tuple{typeof(row_group_slots), Tuple{Vararg{var"#s142", N}} where var"#s142"<:(AbstractVector{T} where T), Tuple{Vararg{var"#s141", N}} where var"#s141"<:(AbstractVector{T} where T), Val{false}, Union{Nothing, Vector{Int64}}, Bool, Bool} where N)
    m.nospecialize |= 3
end
let m = which(Tuple{typeof(hashrows_col!), Vector{UInt64}, Vector{Bool}, AbstractVector{T} where T, Any, Bool})
    m.nospecialize |= 12
end
let m = only(methods(Base.bodyfunction(which(DataFrame, (Any,)))))
    m.nospecialize |= 5
end
let m = which(Core.kwfunc(Sort.defalg), (Any,typeof(Base.Sort.defalg),AbstractDataFrame,Ordering,))
    m.nospecialize |= 13
end
let m = only(methods(Base.bodyfunction(which(sort, (AbstractDataFrame,Any,)))))
    m.nospecialize |= 396
end
let m = which(Tuple{Type{SubDataFrame}, SubDataFrame, Any, Any})
    m.nospecialize |= 7
end
let m = which(Tuple{typeof(lt), DFPerm, Any, Any})
    m.nospecialize |= 1
end
let m = which(Tuple{typeof(_add_col_check_copy), DataFrame, AbstractDataFrame, Union{Nothing, Int64, AbstractVector{Int64}, AsTable}, Bool, Any, Symbol, AbstractVector{T} where T})
    m.nospecialize |= 86
end
let m = which(Tuple{typeof(getindex), DataFrame, Colon, Union{Colon, Regex, AbstractVector{T} where T, All, Between, Cols, InvertedIndex}})
    m.nospecialize |= 4
end
let m = which(Core.kwfunc(sortperm), (Any,typeof(sortperm),AbstractDataFrame,Any,))
    m.nospecialize |= 13
end
let m = which(Tuple{typeof(select), GroupedDataFrame, Vararg{Any, N} where N})
    m.nospecialize |= 2
end
let m = which(Tuple{Type{SubIndex}, AbstractIndex, AbstractUnitRange{Int64}})
    m.nospecialize |= 2
end
let m = which(Tuple{typeof(lookupname), Dict{Symbol, Int64}, Symbol})
    m.nospecialize |= 2
end
let m = only(methods(Base.bodyfunction(which(issorted, (AbstractDataFrame,Any,)))))
    m.nospecialize |= 71
end
let m = only(methods(Base.bodyfunction(which(sortperm, (AbstractDataFrame,Any,)))))
    m.nospecialize |= 196
end
let m = which(Core.kwfunc(rightjoin), (Any,typeof(rightjoin),AbstractDataFrame,AbstractDataFrame,))
    m.nospecialize |= 1
end
let m = which(Core.kwfunc(push!), (Any,typeof(push!),DataFrame,Union{AbstractDict, NamedTuple},))
    m.nospecialize |= 9
end
let m = which(Tuple{Type{SubDataFrame}, SubDataFrame, Colon, Any})
    m.nospecialize |= 5
end
let m = which(Tuple{typeof(do_append!), Any, Any, Any})
    m.nospecialize |= 7
end
let m = which(Tuple{typeof(copyto!), AbstractDataFrame, Base.Broadcast.Broadcasted})
    m.nospecialize |= 3
end
let m = which(Tuple{Type{DataFrameRow}, DataFrame, Integer, Any})
    m.nospecialize |= 6
end
let m = which(Core.kwfunc(leftjoin), (Any,typeof(leftjoin),AbstractDataFrame,AbstractDataFrame,))
    m.nospecialize |= 1
end
let m = which(Core.kwfunc(manipulate), (Any,typeof(manipulate),DataFrame,Vararg{Any, N} where N,))
    m.nospecialize |= 9
end
let m = which(Tuple{typeof(do_call), Union{Function, Type}, AbstractVector{var"#s249"} where var"#s249"<:Integer, AbstractVector{var"#s248"} where var"#s248"<:Integer, AbstractVector{var"#s146"} where var"#s146"<:Integer, GroupedDataFrame, Nothing, Integer})
    m.nospecialize |= 1
end
let m = which(Core.kwfunc(reduce), (Any,typeof(reduce),typeof(vcat),Union{Tuple{AbstractDataFrame, Vararg{AbstractDataFrame, N} where N}, AbstractVector{var"#s18"} where var"#s18"<:AbstractDataFrame},))
    m.nospecialize |= 9
end
let m = which(Tuple{typeof(funname), Any})
    m.nospecialize |= 1
end
let m = which(Core.kwfunc(Type), (Any,Type{DataFrame},AbstractVector{var"#s31"} where var"#s31"<:(AbstractVector{T} where T),AbstractVector{Symbol},))
    m.nospecialize |= 13
end
let m = only(methods(Base.bodyfunction(which(manipulate, (SubDataFrame,Vararg{Any, N} where N,)))))
    m.nospecialize |= 48
end
let m = only(methods(Base.bodyfunction(which(rightjoin, (AbstractDataFrame,AbstractDataFrame,)))))
    m.nospecialize |= 25
end
let m = which(Tuple{ByRow, NamedTuple})
    m.nospecialize |= 1
end
let m = only(methods(Base.bodyfunction(which(push!, (DataFrame,Union{AbstractDict, NamedTuple},)))))
    m.nospecialize |= 16
end
let m = which(Tuple{typeof(getindex), SubDataFrame, typeof(!), Union{Colon, Regex, AbstractVector{T} where T, All, Between, Cols, InvertedIndex}})
    m.nospecialize |= 5
end
let m = which(Tuple{typeof(fillfirst!), Any, AbstractVector{T} where T, AbstractVector{T} where T, GroupedDataFrame})
    m.nospecialize |= 15
end
let m = only(methods(Base.bodyfunction(which(leftjoin, (AbstractDataFrame,AbstractDataFrame,)))))
    m.nospecialize |= 25
end
let m = which(Tuple{typeof(update_row_maps!), AbstractDataFrame, AbstractDataFrame, RowGroupDict, Union{Nothing, RowIndexMap}, Union{Nothing, RowIndexMap}, Union{Nothing, RowIndexMap}, Union{Nothing, Vector{Bool}}, Tuple{Vararg{AbstractVector{T} where T, N} where N}, Tuple{Vararg{AbstractVector{T} where T, N} where N}})
    m.nospecialize |= 464
end
let m = which(Tuple{typeof(groupreduce_init), Any, Any, Any, AbstractVector{U}, GroupedDataFrame} where U)
    m.nospecialize |= 11
end
let m = which(Tuple{typeof(getindex), SubDataFrame, typeof(!), Union{AbstractString, Signed, Symbol, Unsigned}})
    m.nospecialize |= 5
end
let m = which(Tuple{typeof(groupreduce), Any, Any, Base.var"#78#79"{typeof(ismissing)}, Any, Bool, AbstractVector{T} where T, GroupedDataFrame})
    m.nospecialize |= 35
end
let m = only(methods(Base.bodyfunction(which(reduce, (typeof(vcat),Union{Tuple{AbstractDataFrame, Vararg{AbstractDataFrame, N} where N}, AbstractVector{var"#s20"} where var"#s20"<:AbstractDataFrame},)))))
    m.nospecialize |= 9
end
let m = which(Tuple{typeof(normalize_selection), AbstractIndex, Pair{var"#s249", var"#s248"} where var"#s248"<:Union{Function, Type} where var"#s249"<:Union{AbstractString, Signed, Symbol, Unsigned}, Bool})
    m.nospecialize |= 3
end
let m = which(Tuple{typeof(_transformation_helper), AbstractDataFrame, Nothing, Any})
    m.nospecialize |= 5
end
let m = which(Tuple{typeof(isagg), Pair{var"#s248", var"#s146"} where var"#s146"<:(Pair{var"#s145", var"#s144"} where var"#s144"<:Union{AbstractString, Symbol} where var"#s145") where var"#s248"<:Union{AbstractString, Signed, Symbol, Unsigned}, GroupedDataFrame})
    m.nospecialize |= 1
end
let m = which(Tuple{typeof(_sortperm), AbstractDataFrame, Algorithm, Union{DFPerm, Perm}})
    m.nospecialize |= 7
end
let m = only(methods(Base.bodyfunction(which(stack, (AbstractDataFrame,Any,Any,)))))
    m.nospecialize |= 200
end
let m = which(Core.kwfunc(manipulate), (Any,typeof(manipulate),SubDataFrame,Vararg{Any, N} where N,))
    m.nospecialize |= 13
end
let m = which(Tuple{typeof(getproperty), AbstractDataFrame, Symbol})
    m.nospecialize |= 3
end
let m = which(Tuple{typeof(_combine_process_pair_astable), Bool, GroupedDataFrame, Dict{Symbol, Tuple{Bool, Int64}}, Vector{TransformationResult}, Union{Nothing, AbstractVector{Int64}}, Union{AbstractVector{Symbol}, Type{AsTable}}, Bool, Any, Union{Function, Type}, Union{Tuple, NamedTuple}})
    m.nospecialize |= 688
end
let m = which(Tuple{typeof(view), AbstractDataFrame, Any, Union{AbstractString, Signed, Symbol, Unsigned}})
    m.nospecialize |= 7
end
let m = which(Core.kwfunc(select), (Any,typeof(select),GroupedDataFrame,Vararg{Any, N} where N,))
    m.nospecialize |= 9
end
let m = which(Tuple{typeof(normalize_selection), AbstractIndex, Pair{var"#s249", var"#s248"} where var"#s248"<:Union{Function, Type} where var"#s249", Bool})
    m.nospecialize |= 3
end
let m = which(Core.kwfunc(combine), (Any,typeof(combine),GroupedDataFrame,Vararg{Union{Regex, AbstractString, Function, Signed, Symbol, Unsigned, Pair, AbstractVector{T} where T, Type, All, Between, Cols, InvertedIndex}, N} where N,))
    m.nospecialize |= 9
end
let m = which(Core.kwfunc(outerjoin), (Any,typeof(outerjoin),AbstractDataFrame,AbstractDataFrame,))
    m.nospecialize |= 1
end
let m = which(Tuple{Type{SubDataFrame}, DataFrame, AbstractVector{Int64}, Any})
    m.nospecialize |= 6
end
let m = only(methods(Base.bodyfunction(which(outerjoin, (AbstractDataFrame,AbstractDataFrame,)))))
    m.nospecialize |= 29
end
let m = which(Tuple{typeof(groupreduce), Any, Any, Any, Any, Bool, AbstractVector{T} where T, GroupedDataFrame})
    m.nospecialize |= 35
end
let m = which(Tuple{typeof(hashrows), Tuple{Vararg{AbstractVector{T} where T, N} where N}, Bool})
    m.nospecialize |= 1
end
let m = which(Core.kwfunc(innerjoin), (Any,typeof(innerjoin),AbstractDataFrame,AbstractDataFrame,))
    m.nospecialize |= 5
end
let m = which(Tuple{typeof(size), AbstractDataFrame, Integer})
    m.nospecialize |= 3
end
let m = which(Tuple{typeof(_combine_multicol), Any, Union{Function, Type}, GroupedDataFrame, Union{Nothing, Tuple, AbstractVector{T} where T, NamedTuple}})
    m.nospecialize |= 9
end
let m = which(Tuple{typeof(view), AbstractDataFrame, Any, Union{Colon, Regex, AbstractVector{T} where T, All, Between, Cols, InvertedIndex}})
    m.nospecialize |= 7
end
let m = only(methods(Base.bodyfunction(which(innerjoin, (AbstractDataFrame,AbstractDataFrame,)))))
    m.nospecialize |= 77
end
let m = which(Tuple{typeof(row_group_slots), Tuple{Vararg{AbstractVector{T} where T, N} where N}, Any, Val, Union{Nothing, Vector{Int64}}, Bool, Bool})
    m.nospecialize |= 3
end
let m = which(Core.kwfunc(manipulate), (Any,typeof(manipulate),SubDataFrame,Union{Colon, Regex, AbstractVector{T} where T, All, Between, Cols, InvertedIndex},))
    m.nospecialize |= 13
end
let m = which(Tuple{Reduce, AbstractVector{T} where T, GroupedDataFrame})
    m.nospecialize |= 1
end
let m = which(Tuple{typeof(normalize_selection), AbstractIndex, Pair{var"#s249", var"#s248"} where var"#s248"<:(Pair{var"#s146", var"#s145"} where var"#s145"<:Union{AbstractString, Symbol} where var"#s146"<:Union{Function, Type}) where var"#s249"<:Union{AbstractString, Signed, Symbol, Unsigned}, Bool})
    m.nospecialize |= 3
end
let m = which(Tuple{typeof(axes), AbstractDataFrame, Integer})
    m.nospecialize |= 3
end
let m = only(methods(Base.bodyfunction(which(_join, (AbstractDataFrame,AbstractDataFrame,)))))
    m.nospecialize |= 617
end
let m = which(Core.kwfunc(_join), (Any,typeof(_join),AbstractDataFrame,AbstractDataFrame,))
    m.nospecialize |= 5
end
let m = which(Tuple{ByRow, Vararg{AbstractVector{T} where T, N} where N})
    m.nospecialize |= 1
end
let m = only(methods(Base.bodyfunction(which(select, (GroupedDataFrame,Vararg{Any, N} where N,)))))
    m.nospecialize |= 65
end
let m = which(Tuple{typeof(row_group_slots), Tuple{Vararg{AbstractVector{T} where T, N} where N}, Val, Union{Nothing, Vector{Int64}}, Bool, Bool})
    m.nospecialize |= 1
end
let m = which(Core.kwfunc(manipulate), (Any,typeof(manipulate),DataFrame,Union{Colon, Regex, AbstractVector{T} where T, All, Between, Cols, InvertedIndex},))
    m.nospecialize |= 9
end
let m = which(Tuple{typeof(groupreduce!), AbstractVector{T} where T, Any, Any, Any, Any, Bool, AbstractVector{T} where T, GroupedDataFrame})
    m.nospecialize |= 87
end
let m = which(Tuple{typeof(getindex), DataFrame, AbstractVector{T}, Union{Colon, Regex, AbstractVector{T} where T, All, Between, Cols, InvertedIndex}} where T)
    m.nospecialize |= 6
end
let m = which(Tuple{typeof(_combine_process_pair_symbol), Bool, GroupedDataFrame, Dict{Symbol, Tuple{Bool, Int64}}, Vector{TransformationResult}, Union{Nothing, AbstractVector{Int64}}, Symbol, Bool, Any, Union{Function, Type}, Union{Tuple, NamedTuple}})
    m.nospecialize |= 656
end
let m = which(Core.kwfunc(select), (Any,typeof(select),AbstractDataFrame,Vararg{Any, N} where N,))
    m.nospecialize |= 13
end
let m = only(methods(Base.bodyfunction(which(select, (AbstractDataFrame,Vararg{Any, N} where N,)))))
    m.nospecialize |= 27
end
let m = which(Tuple{typeof(combine), GroupedDataFrame, Vararg{Union{Regex, AbstractString, Function, Signed, Symbol, Unsigned, Pair, AbstractVector{T} where T, Type, All, Between, Cols, InvertedIndex}, N} where N})
    m.nospecialize |= 2
end
let m = which(Core.kwfunc(Type), (Any,Type{DataFrame},))
    m.nospecialize |= 1
end
let m = which(Tuple{typeof(_combine_with_first), Union{AbstractDataFrame, NamedTuple, DataFrameRow}, Union{Function, Type}, GroupedDataFrame, Union{Nothing, Tuple, AbstractVector{T} where T, NamedTuple}, Val, Union{Nothing, AbstractVector{var"#s249"} where var"#s249"<:Integer}})
    m.nospecialize |= 59
end
let m = only(methods(Base.bodyfunction(which(DataFrame, ()))))
    m.nospecialize |= 1
end
let m = which(Tuple{typeof(do_call), Union{Function, Type}, AbstractVector{var"#s249"} where var"#s249"<:Integer, AbstractVector{var"#s248"} where var"#s248"<:Integer, AbstractVector{var"#s146"} where var"#s146"<:Integer, GroupedDataFrame, Tuple{AbstractVector{T} where T}, Integer})
    m.nospecialize |= 33
end
let m = only(methods(Base.bodyfunction(which(combine, (GroupedDataFrame,Vararg{Union{Regex, AbstractString, Function, Signed, Symbol, Unsigned, Pair, AbstractVector{T} where T, Type, All, Between, Cols, InvertedIndex}, N} where N,)))))
    m.nospecialize |= 32
end
let m = which(Core.kwfunc(_combine_prepare), (Any,typeof(_combine_prepare),GroupedDataFrame,Vararg{Union{Regex, AbstractString, Function, Signed, Symbol, Unsigned, Pair, AbstractVector{T} where T, Type, All, Between, Cols, InvertedIndex}, N} where N,))
    m.nospecialize |= 9
end
