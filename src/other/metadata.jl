# Defining behavior for DataFrames metadata
abstract type AbstractMetaData end

mutable struct MetaData <: AbstractMetaData
    columndata::Dict{Symbol, Vector}
end
    
MetaData() = MetaData(Dict{Symbol,Vector}())

Base.isequal(x::MetaData, y::MetaData) = isequal(x.columndata, y.columndata)
Base.:(==)(x::MetaData, y::MetaData) = isequal(x, y)

Base.copy(x::MetaData) = MetaData(copy(x.columndata))
Base.deepcopy(x::MetaData) = MetaData(copy(x.columndata)) # field is immutable

function Base.getindex(x::MetaData, col_inds::AbstractVector)
    new_columndata = copy(x.columndata)
    for key in keys(new_columndata)
        new_columndata[key] = new_columndata[key][col_inds]
    end 
    MetaData(new_columndata)
end

function Base.permute!(x::MetaData, p::AbstractVector)
    for key in keys(x.columndata)
        x.columndata[key] = permute!(x.columndata[key], p)
    end 
    nothing
end

function Base.permute(x::MetaData, p::AbstractVector)
    new_metadata = copy(x)
    permute!(new_metadata, p)
end


function newfield!(x::MetaData, ncol::Int, field::Symbol, info)
    x.columndata[field] = Vector{Union{typeof(info), Nothing}}([nothing for i in 1:ncol])
end

function addmeta!(x::MetaData, col_ind::Int, ncol::Int, field::Symbol, info)
    if !haskey(x.columndata, field)
        newfield!(x, ncol, field, info)
    end
    x.columndata[field][col_ind] = info
    return nothing
end

# For creating a new column in the dataframe
function Base.push!(x::MetaData, info)
    for key in keys(x.columndata)
        push!(x.columndata[key], info)
    end
end

function Base.insert!(x::MetaData, col_ind::Int, item)
    for key in keys(x.columndata)
        insert!(x.columndata[key], col_ind, item)
    end
end

function Base.append!(leftmeta::MetaData, rightmeta::MetaData, ncol_left::Int, ncol_right::Int)
    notonleft = setdiff(keys(rightmeta.columndata), keys(leftmeta.columndata))
    notonright = setdiff(keys(leftmeta.columndata), keys(rightmeta.columndata))

    for field in notonleft
        newfield!(leftmeta, ncol_left, field, nothing)
    end

    for field in notonright
        newfield!(rightmeta, ncol_right, field, nothing)
    end

    for key in keys(leftmeta.columndata)
        leftmeta.columndata[key] = 
            vcat(leftmeta.columndata[key], rightmeta.columndata[key])
    end
end

function append(leftmeta::MetaData, rightmeta::MetaData)
    append!(copy(leftmeta), rightmeta)
end

# deleting columns is handled by get_index? 
function getmeta(x::MetaData, col_ind::Int, field::Symbol)
    if haskey(x.columndata, field)
        return x.columndata[field][col_ind]
    else
        error("The field does not exist")
    end
end