# normalize_selection function makes sure that whatever input format of idx is it
# will end up in one of four canonical forms
# 1) AbstractVector{Int}
# 2) Pair{Int, <:Pair{<:Base.Callable, <:Union{Symbol, Vector{Symbol}, Type{AsTable}}}}
# 3) Pair{AbstractVector{Int}, <:Pair{<:Base.Callable, <:Union{Symbol, AbstractVector{Symbol}, Type{AsTable}}}}
# 4) Pair{AsTable, <:Pair{<:Base.Callable, <:Union{Symbol, Vector{Symbol}, Type{AsTable}}}}
# 5) Callable

broadcast_pair(df, @nospecialize(p::Any)) = p

function broadcast_pair(df, @nospecialize(p::Pair))
    src, second = p
    src_broadcast = src isa Union{InvertedIndices.BroadcastedInvertedIndex,
                                  DataAPI.BroadcastedSelector}
    second_broadcast = second isa Union{InvertedIndices.BroadcastedInvertedIndex,
                                        DataAPI.BroadcastedSelector}
    if second isa Pair
        fun, dst = second
        dst_broadcast = dst isa Union{InvertedIndices.BroadcastedInvertedIndex,
                                      DataAPI.BroadcastedSelector}
        if src_broadcast || dst_broadcast
            new_src = src_broadcast ? names(df, src.sel) : src
            new_dst = dst_broadcast ? names(df, dst.sel) : dst
            new_p = new_src .=> fun .=> new_dst
            return isempty(new_p) ? [] : new_p
        else
            return p
        end
    else
        if src_broadcast || second_broadcast
            new_src = src_broadcast ? names(df, src.sel) : src
            new_second = second_broadcast ? names(df, second.sel) : second
            new_p = new_src .=> new_second
            return isempty(new_p) ? [] : new_p
        else
            return p
        end
    end
end

# this is needed in broadcasting when one of dimensions has length 0
# as then broadcasting produces Matrix{Any} rather than Matrix{<:Pair}
broadcast_pair(df, @nospecialize(p::AbstractMatrix)) =
    isempty(p) ? [] : p

function broadcast_pair(df, @nospecialize(p::AbstractVecOrMat{<:Pair}))
    isempty(p) && return []
    need_broadcast = false

    src = first.(p)
    first_src = first(src)
    if first_src isa Union{InvertedIndices.BroadcastedInvertedIndex,
                           DataAPI.BroadcastedSelector}
        if any(!=(first_src), src)
            throw(ArgumentError("when broadcasting column selector it must " *
                                "have a constant value"))
        end
        need_broadcast = true
        new_names = names(df, first_src.sel)
        if !(length(new_names) == size(p, 1) || size(p, 1) == 1)
            throw(ArgumentError("broadcasted dimension does not match the " *
                                "number of selected columns"))
        end
        new_src = new_names
    else
        new_src = src
    end

    second = last.(p)
    first_second = first(second)
    if first_second isa Union{InvertedIndices.BroadcastedInvertedIndex,
                              DataAPI.BroadcastedSelector}
        if any(!=(first_second), second)
            throw(ArgumentError("when using broadcasted column selector it " *
                                "must have a constant value"))
        end
        need_broadcast = true
        new_names = names(df, first_second.sel)
        if !(length(new_names) == size(p, 1) || size(p, 1) == 1)
            throw(ArgumentError("broadcasted dimension does not match the " *
                                "number of selected columns"))
        end
        new_second = new_names
    else
        if first_second isa Pair
            fun, dst = first_second
            if dst isa Union{InvertedIndices.BroadcastedInvertedIndex,
                             DataAPI.BroadcastedSelector}
                if !all(x -> x isa Pair && last(x) == dst, second)
                    throw(ArgumentError("when using broadcasted column selector " *
                                        "it must have a constant value"))
                end
                need_broadcast = true
                new_names = names(df, dst.sel)
                if !(length(new_names) == size(p, 1) || size(p, 1) == 1)
                    throw(ArgumentError("broadcasted dimension does not match the " *
                                        "number of selected columns"))
                end
                new_dst = new_names
                new_second = first.(second) .=> new_dst
            else
                new_second = second
            end
        else
            new_second = second
        end
    end

    if need_broadcast
        new_p = new_src .=> new_second
        return isempty(new_p) ? [] : new_p
    else
        return p
    end
end

# add a method to funname defined in other/utils.jl
funname(row::ByRow) = funname(row.fun)

make_pair_concrete(@nospecialize(x::Pair)) =
    make_pair_concrete(x.first) => make_pair_concrete(x.second)
make_pair_concrete(@nospecialize(x)) = x

normalize_selection(idx::AbstractIndex, @nospecialize(sel), renamecols::Bool) =
    try
        idx[sel]
    catch e
        if e isa MethodError && e.f === getindex && e.args === (idx, sel)
            throw(ArgumentError("Unrecognized column selector $sel in AsTable constructor"))
        else
            rethrow(e)
        end
    end

normalize_selection(idx::AbstractIndex, @nospecialize(sel::Base.Callable), renamecols::Bool) = sel
normalize_selection(idx::AbstractIndex, sel::Colon, renamecols::Bool) = idx[:]

normalize_selection(idx::AbstractIndex, sel::Pair{typeof(nrow), Symbol},
                    renamecols::Bool) =
    length(idx) == 0 ? (Int[] => (() -> 0) => last(sel)) : (1 => length => last(sel))
normalize_selection(idx::AbstractIndex, sel::Pair{typeof(nrow), <:AbstractString},
                    renamecols::Bool) =
    normalize_selection(idx, first(sel) => Symbol(last(sel)), renamecols)
normalize_selection(idx::AbstractIndex, sel::typeof(nrow), renamecols::Bool) =
    normalize_selection(idx, nrow => :nrow, renamecols)

normalize_selection(idx::AbstractIndex, sel::Pair{typeof(eachindex), Symbol},
                    renamecols::Bool) =
    length(idx) == 0 ? (Int[] => (() -> Base.OneTo(0)) => last(sel)) : (1 => eachindex => last(sel))
normalize_selection(idx::AbstractIndex, sel::Pair{typeof(eachindex), <:AbstractString},
                    renamecols::Bool) =
    normalize_selection(idx, first(sel) => Symbol(last(sel)), renamecols)
normalize_selection(idx::AbstractIndex, sel::typeof(eachindex), renamecols::Bool) =
    normalize_selection(idx, eachindex => :eachindex, renamecols)


function normalize_selection(idx::AbstractIndex, sel::ColumnIndex, renamecols::Bool)
    c = idx[sel]
    return c => identity => _names(idx)[c]
end

function normalize_selection(idx::AbstractIndex, sel::Pair{<:ColumnIndex, Symbol},
                             renamecols::Bool)
    c = idx[first(sel)]
    return c => identity => last(sel)
end

normalize_selection(idx::AbstractIndex, sel::Pair{<:ColumnIndex, <:AbstractString},
                    renamecols::Bool) =
    normalize_selection(idx, first(sel) => Symbol(last(sel)), renamecols)

normalize_selection(idx::AbstractIndex, sel::Pair{<:ColumnIndex,
                                                  <:Union{AbstractVector{Symbol},
                                                          AbstractVector{<:AbstractString}}},
                    renamecols::Bool) =
    normalize_selection(idx, first(sel) => identity => last(sel), renamecols)

function normalize_selection(idx::AbstractIndex,
                             @nospecialize(sel::Pair{<:ColumnIndex,
                                                     <:Pair{<:Base.Callable,
                                                            <:Union{Symbol, AbstractString}}}),
                             renamecols::Bool)
    src, (fun, dst) = sel
    return idx[src] => fun => Symbol(dst)
end

function normalize_selection(idx::AbstractIndex,
                             @nospecialize(sel::Pair{<:Any,
                                                     <:Pair{<:Base.Callable,
                                                            <:Union{Symbol, AbstractString, DataType,
                                                                    AbstractVector{Symbol},
                                                                    AbstractVector{<:AbstractString},
                                                                    Function}}}),
                             renamecols::Bool)
    lls = last(last(sel))

    if lls isa DataType
        lls === AsTable || throw(ArgumentError("Only DataType supported as target is AsTable"))
    end

    if first(sel) isa AsTable
        rawc = first(sel).cols
        wanttable = true
    else
        rawc = first(sel)
        wanttable = false
    end

    if rawc isa AbstractVector{Int}
        c = rawc
    elseif rawc isa Union{AbstractVector{Symbol}, AbstractVector{<:AbstractString}}
        c = [idx[n] for n in rawc]
    else
        c = try
                idx[rawc]
            catch e
                if e isa MethodError && e.f === getindex && e.args === (idx, rawc)
                    throw(ArgumentError("Unrecognized column selector: $rawc"))
                else
                    rethrow(e)
                end
            end
    end

    if lls isa Function
        fun_colnames = _names(idx)[c]
        # if AsTable was used as source we always treat it as multicolumn selector
        if wanttable && fun_colnames isa Symbol
            fun_colnames = [fun_colnames]
        end
        lls = lls(string.(fun_colnames))
        if !(lls isa Union{Symbol, AbstractString, AbstractVector{Symbol},
                           AbstractVector{<:AbstractString}})
            throw(ArgumentError("function producing target column names must " *
                                "return a Symbol, a string, a vector of Symbols " *
                                "or a vector of strings"))
        end
    end
    if lls isa AbstractString
        combine_target_col = Symbol(lls)
    elseif lls isa AbstractVector{<:AbstractString}
        combine_target_col = Symbol.(lls)
    else
        combine_target_col = lls
    end

    if combine_target_col isa AbstractVector{Symbol}
        allunique(combine_target_col) || throw(ArgumentError("target column names must be unique"))
    end

    if wanttable
        combine_src = AsTable(c)
    else
        combine_src = (length(c) == 1 ? only(c) : c)
    end

    combine_func = first(last(sel))

    return combine_src => combine_func => combine_target_col
end

function normalize_selection(idx::AbstractIndex,
                             @nospecialize(sel::Pair{<:ColumnIndex, <:Base.Callable}), renamecols::Bool)
    c = idx[first(sel)]
    fun = last(sel)

    if fun === AsTable
        return normalize_selection(idx, first(sel) => identity => AsTable, renamecols)
    end

    if renamecols
        newcol = Symbol(_names(idx)[c], "_", funname(fun))
    else
        newcol = _names(idx)[c]
    end
    return c => fun => newcol
end

function normalize_selection(idx::AbstractIndex,
                             @nospecialize(sel::Pair{<:Any, <:Base.Callable}), renamecols::Bool)
    if first(sel) isa AsTable
        rawc = first(sel).cols
        wanttable = true
    else
        rawc = first(sel)
        wanttable = false
    end
    if rawc isa AbstractVector{Int}
        c = rawc
    elseif rawc isa Union{AbstractVector{Symbol}, AbstractVector{<:AbstractString}}
        c = [idx[n] for n in rawc]
    else
        c = try
                idx[rawc]
            catch e
                if e isa MethodError && e.f === getindex && e.args === (idx, rawc)
                    throw(ArgumentError("Unrecognized column selector: $rawc"))
                else
                    rethrow(e)
                end
            end
    end
    fun = last(sel)

    fun === AsTable && throw(ArgumentError("Passing AsTable in $sel is not supported"))

    if length(c) > 3
        prefix = join(@views(_names(idx)[c[1:2]]), '_')
        if renamecols
            newcol = Symbol(prefix, "_etc_", funname(fun))
        else
            newcol = Symbol(prefix, "_etc")
        end
    elseif isempty(c)
        renamecols || throw(ArgumentError("when renamecols=false target column name " *
                                          "must be passed if there are no input columns"))
        newcol = Symbol(funname(fun))
    else
        prefix = join(view(_names(idx), c), '_')
        if renamecols
            newcol = Symbol(prefix, '_', funname(fun))
        else
            newcol = Symbol(prefix)
        end
    end

    if wanttable
        combine_src = AsTable(c)
    else
        combine_src = (length(c) == 1 ? only(c) : c)
    end

    return combine_src => fun => newcol
end
