# TODO: This is a binary implementation
# TODO: Need an implementation that assumes everything is a string as well
# TODO: Handle NA values somehow.
# This function is close to what R calls scan()

# Note that width and buffer are ignored for numeric types
function readentry!{T <: Real}(io::IO,
	                           ::Type{T},
	                           width::Integer,
	                           buffer::Vector{Uint8})
	return read(io, T)
end

# Note that width is ignored for numeric types
function writeentry!(io::IO, val::Real, width::Integer)
	write(io, val)
	return
end

function readentry!{T <: ByteString}(io::IO,
		                             ::Type{T},
	                                 width::Integer,
	                                 buffer::Vector{Uint8})
	for byte in 1:width
		buffer[byte] = read(io, Uint8)
	end
	index = 0
	for byte in 1:width
		if buffer[byte] == 0x00
			index = byte
			break
		end
	end
	if index == 0
		error("Invalid string found in input")
	end
	return UTF8String(buffer[1:(index - 1)])
end

function writeentry!(io::IO, val::ByteString, width::Integer)
	bytes = val.data
	nbytes = length(bytes)
	for i in 1:nbytes
		write(io, bytes[i])
	end
	for i in 1:(width - nbytes)
		write(io, 0x00)
	end
	return
end

function writefwfbin(path::String, adf::AbstractDataFrame)
	io = open(path, "w")
	nrows, ncols = size(adf)
	widths = Array(Int, ncols)
	for j in 1:ncols
		T = eltype(adf[j])
		if issubtype(T, Number)
			widths[j] = sizeof(T)
		elseif issubtype(T, ByteString)
			width = 0
			for i in 1:nrows
				width = max(width, sizeof(adf[j][i]))
			end
			widths[j] = width + 1 # Always include null padding, even for longest string
		else
			throw(ArgumentError("Type cannot be serialized"))
		end
	end
	for i in 1:nrows
		for j in 1:ncols
			writeentry!(io, adf[i, j], widths[j])
		end
	end
	close(io)
end

# TODO: Handle header line if any
# TODO: Allow type information to be embedded in line?
function readfwfbin(path::String,
	                types::Vector{DataType},
	                widths::Vector{Int})
	io = open(path, "r")

	n = length(types)
	if length(widths) != n
		throw(ArgumentError("Types and widths must have the same length"))
	end

	cols = Array(Any, n)
	for j in 1:n
		cols[j] = Array(types[j], 0)
	end

	buffer = Array(Uint8, maximum(widths))

	while !eof(io)
		for j in 1:n
			entry = readentry!(io, types[j], widths[j], buffer)
			push!(cols[j], entry)
		end
	end

	close(io)

	return DataFrame(cols)
end
