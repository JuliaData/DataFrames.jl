abstract CDataFrame <: AbstractDataFrame

expr_typeof(d::DataType) = :($(d.name))

symb(x::Type) = x.name.name
symb(x::Int) = x
symb(x) = symbol(x)

function expr_typeof(d)
    # return an expression representing the type of d
    # This is used to build up the member list in the CDataFrame type
    t = typeof(d)
    return :($(t.name.name){$(map(symb, t.parameters)...)})
end


function cdataframe(df::AbstractDataFrame)
    # t = symbol("CDataFrame" * string(gensym()))
    t = symbol("CDataFrame" * string(rand(Uint16)))
    typedef = quote
        type $(t) <: CDataFrame
        end
    end
    type_exprs = Any[]
    for i in 1:ncol(df)
        push!(type_exprs, :($(symbol(colnames(df)[i]))::$(expr_typeof(df[:,i]))))
    end
    typedef.args[2].args[3].args = type_exprs
    # typedef.args[2].args[3].args = Any[symbol(x) for x in colnames(df)]
    eval(typedef)
    T = eval(:($t))
    a = Any[]
    for colname in colnames(df)
        push!(a, df[colname])
    end
    T(a...)
end

cdataframe(df::CDataFrame) = df

cdataframe(df::CDataFrame; kwargs...) = cdataframe(cbind(DataFrame(df), DataFrame(; kwargs...)))

DataFrame(df::CDataFrame) = DataFrame(Any[df[i] for i in 1:ncol(df)], colnames(df))

colnames(df::CDataFrame) = [string(x)::ByteString for x in names(typeof(df))]
colsymbols(df::CDataFrame) = [x::Symbol for x in names(typeof(df))]

nrow(df::CDataFrame) = ncol(df) > 0 ? length(getfield(df, names(typeof(df))[1])) : 0
ncol(df::CDataFrame) = length(typeof(df).types)

index(df::CDataFrame) = Index(colnames(df))

function Base.getindex(df::CDataFrame, col_ind::Real)
    getfield(df, colsymbols(df)[col_ind])
end

function Base.getindex(df::CDataFrame, col_ind::String)
    getfield(df, symbol(col_ind))
end

function Base.getindex(df::CDataFrame, col_ind::Symbol)
    getfield(df, col_ind)
end

function Base.getindex{T <: ColumnIndex}(df::CDataFrame, col_inds::AbstractVector{T})
    CDataFrame(DataFrame(df)[col_inds])
end

function Base.getindex(df::CDataFrame, row_ind::Real, col_ind::ColumnIndex)
    df[col_ind][row_ind]
end

# df[SingleRowIndex, MultiColumnIndex] => (Sub)?DataFrame
function Base.getindex{T <: ColumnIndex}(df::CDataFrame, row_ind::Real, col_inds::AbstractVector{T})
    cdataframe(DataFrame(df)[row_ind, col_inds])
end

# df[MultiRowIndex, SingleColumnIndex] => (Sub)?AbstractDataVector
function Base.getindex{T <: Real}(df::CDataFrame, row_inds::AbstractVector{T}, col_ind::ColumnIndex)
    df[col_ind][row_inds]
end

# df[MultiRowIndex, MultiColumnIndex] => (Sub)?DataFrame
function Base.getindex{R <: Real, T <: ColumnIndex}(df::CDataFrame, row_inds::AbstractVector{R}, col_inds::AbstractVector{T})
    cdataframe(DataFrame(df)[row_inds, col_inds])
end

# two-argument form, two dfs, references only
function Base.hcat(df1::CDataFrame, df2::CDataFrame)
    cdataframe(hcat(DataFrame(df1), DataFrame(df2)))
end

function Base.hcat(df::CDataFrame, x)
    cdataframe(hcat(DataFrame(df), DataFrame(x)))
end

Base.similar(df::CDataFrame, dims) = cdataframe(similar(DataFrame(df), dims))
