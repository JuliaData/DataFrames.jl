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

function newfield!(x::MetaData, ncol::Int, field::Symbol,)
    x.columndata[field] = Vector{Any}([nothing for i in 1:ncol]) 
end

function addmeta!(x::MetaData, col_ind::Int, ncol::Int, field::Symbol, info)
    if !haskey(x.columndata, field)
        newfield!(x, ncol, field)
    end
    x.columndata[field][col_ind] = info
end

function getlabel(x::MetaData, col_ind::Int)
    if haskey(x.columndata, :label)
        return x.columndata[:label][col_ind]
    else 
        return ""
    end
end

function addcolumn!(x::MetaData)
    for key in keys(x.columndata)
        push!(x.columndata[key], "")
    end
end

# For creating a new column
function addcolumn!(x::MetaData)
    for key in keys(x.columndata)
        push!(x.columndata[key], "")
    end
end

function Base.insert!(x::MetaData, col_ind::Int, item)
    for key in keys(x.columndata)
        insert!(x.columndata[key], col_ind, item)
    end
end

function Base.append!(leftmeta::MetaData, rightmeta::MetaData)
    notonleft = setdiff(keys(leftmeta.columndata), keys(rightmeta.columndata))
    notonright = setdiff(keys(rightmeta.columndata), keys(leftmeta.columndata))

    for x in notonleft
        newfield!(leftmeta, x)
    end

    for x in notonright
        newfield!(rightmeta, x)
    end

    for key in keys(leftmeta.columndata)
        leftmeta.columndata[key] = 
            vcat(leftmeta.columndata[key], rightmeta.columndata[key])
    end
end

function append(leftmeta::MetaData, rightmeta::MetaData)
    newmeta = copy(leftmeta)
    vcat!(newmeta, rightmeta)
end

# deleting columns is handled by get_index? 

function getmeta(x::MetaData, field::Symbol, col_ind::Int)
    if haskey(x.columndata, field)
        return x.columndata[field][col_ind]
    else
        return "Field does not exist"
    end
end












