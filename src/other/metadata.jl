# Defining behavior for DataFrames metadata
abstract type AbstractMetaData end

mutable struct MetaData <: AbstractMetaData
	columndata::Dict{Symbol, Vector{String}}
end

MetaData() = MetaData(Dict{Symbol,Vector{String}}())

# We would like metadata to be a vector. However we don't want to have to 
# initiate a whole vector of Dicts when a new dataframe is created. Consequently
# we have to have clever ways of subsetting and reordering our Dict of Dicts
# just like an array
# I think a decent way to do this is to have a the columndata dict go to and
# from a real array easily. 

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
	x.columndata[field] = ["" for i in 1:ncol] 
end

function addmeta!(x::MetaData, col_ind::Int, ncol::Int, field::Symbol, info::String)
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

function insert!(x::MetaData, col_ind::Int)
	for key in keys(x.columndata)
		insert!(x.columndata[key], col_ind, "")
	end
end

function Base.vcat(left::Metadata, right::MetaData)
	notonleft = setdiff(keys(left.columndata), keys(right.columndata))
	notonright = setdiff(keys(right.columndata), keys(left.columndata))

	for x in notonleft
		newfield!(left, x)
	end

	for x in notonright
		newfield!(right, x)
	end

	# now they should have the same fields 
	new_metadata = MetaData
	for key in keys(left)
		new_metadata.columndata[key] = 
			vcat(left.columndata[key], right.columndata.[key])
	end
end

# deleting columns is handled by get_index? 

function getmeta(x::MetaData, field::Symbol, col_ind::Int)
	if haskey(x.columndata, field)
		return x.columndata[field][col_ind]
	else
		return "Field does not exist"
	end
end












