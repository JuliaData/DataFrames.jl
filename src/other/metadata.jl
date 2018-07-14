# Defining behavior for DataFrames metadata
struct MetaData 
    dict::Dict{Symbol, Vector}
end
    
MetaData() = MetaData(Dict{Symbol,Vector}())

Base.isequal(x::MetaData, y::MetaData) = isequal(x.dict, y.dict)
Base.:(==)(x::MetaData, y::MetaData) = isequal(x, y)

Base.copy(x::MetaData) = MetaData(copy(x.dict))
Base.deepcopy(x::MetaData) = MetaData(copy(x.dict)) # field is immutable

function Base.getindex(x::MetaData, col_inds::AbstractVector)
    new_dict = copy(x.dict)
    for key in keys(new_dict)
        new_dict[key] = new_dict[key][col_inds]
    end 
    MetaData(new_dict)
end

function Base.permute!(x::MetaData, p::AbstractVector)
    for key in keys(x.dict)
        x.dict[key] = permute!(x.dict[key], p)
    end 
    nothing
end

function Base.permute(x::MetaData, p::AbstractVector)
    new_metadata = copy(x)
    permute!(new_metadata, p)
end


function newfield!(x::MetaData, ncol::Int, field::Symbol, info)
    x.dict[field] = Union{typeof(info), Nothing}[nothing for i in 1:ncol]
end

function addmeta!(x::MetaData, col_ind::Int, ncol::Int, field::Symbol, info)
    if !haskey(x.dict, field)
        newfield!(x, ncol, field, info)
    end
    x.dict[field][col_ind] = info
end

# For creating a new column in the dataframe
function Base.push!(x::MetaData, info)
    for key in keys(x.dict)
        push!(x.dict[key], info)
    end
end

function Base.insert!(x::MetaData, col_ind::Int, item)
    for key in keys(x.dict)
        insert!(x.dict[key], col_ind, item)
    end
end

function Base.merge!(leftmeta::MetaData, rightmeta::MetaData, leftindex::Index, rightindex::Index)
    # Find the unique columns on the right 
    right_and_not_left_names = setdiff(names(rightindex), names(leftindex))
    right_and_not_left_cols = rightindex[right_and_not_left_names]
    # this imitates what's going on with the parent dataframes in merge!
    rightmeta = rightmeta[right_and_not_left_cols]
    rightindex = rightindex[right_and_not_left_names]
    # Find the difference in the keys and allocate if needed
    notonleft = setdiff(keys(rightmeta.dict), keys(leftmeta.dict))
    notonright = setdiff(keys(leftmeta.dict), keys(rightmeta.dict))

    for field in notonleft
        newfield!(leftmeta, length(leftindex), field, nothing)
    end

    for field in notonright
        newfield!(rightmeta, length(rightindex), field, nothing)
    end

    for key in keys(leftmeta.dict)
        leftmeta.dict[key] = 
            vcat(leftmeta.dict[key], rightmeta.dict[key])
    end
end

function append(leftmeta::MetaData, rightmeta::MetaData)
    append!(copy(leftmeta), rightmeta)
end

# deleting columns is handled by get_index? 
function getmeta(x::MetaData, col_ind::Int, field::Symbol)
    if haskey(x.dict, field)
        return x.dict[field][col_ind]
    else
        error("The field does not exist")
    end
end