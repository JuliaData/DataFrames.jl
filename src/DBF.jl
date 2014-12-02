# Read DBF files in xBase format
# Files written in this format have the extension .dbf
# Implemented: dBase III+ (w/o memo)

type DBFFieldDescriptor
	nam::ASCIIString
	typ::DataType
	len::Int8
	dec::Int8
end

type DBFHeader
	version::Uint8
	lastUpdate::String
	records::Int32
	hsize::Int16
	rsize::Int16
	incomplete::Bool
	encrypted::Bool
	mdx::Bool
	langId::Uint8
	fields::Array{DBFFieldDescriptor, 1}
end

function dbf_field_type(fld::Char, dec::Uint8)
	rt = Nothing
	if fld == 'C'
		rt = String
	elseif fld == 'D'
		rt = String
	elseif fld == 'N'
		if dec > 0
			rt = Float64
		else
			rt = Int
		end
	elseif fld == 'F' || fld == 'O'
		rt = Float64
	elseif fld == 'I' || fld == '+'
		rt = Integer
	elseif fld == 'L'
		rt = Bool
	else
		 warn("Unknown record type: $(fld)")
	end
	return rt
end

function read_dbf_field(io::IO)
	field_name = strip(replace((ascii(read(io, Uint8, 11))), '\0', ' ')) # 0x00
	field_type = read(io, Char)  # 0x0B
	read(io, Int32) # skip 0x0C
	field_len = read(io, Uint8) # 0x10
	field_dec = read(io, Uint8) # 0x11
	read(io, Uint8, 14) # reserved
	return DBFFieldDescriptor(field_name, dbf_field_type(field_type, field_dec), field_len, field_dec)
end

function read_dbf_header(io::IO)
	ver = read(io, Uint8)
	date = read(io, Uint8, 3) # 0x01
	last_update = @sprintf("%4d%02d%02d", date[1]+1900, date[2], date[3])
	records = read(io, Int32) # 0x04
	hsize = read(io, Int16) # 0x08
	rsize = read(io, Int16) # 0x0A
	read(io, Int16) # reserved # 0x0C
	incomplete = bool(read(io, Uint8)) # 0x0E
	encrypted = bool(read(io, Uint8)) # 0x0F
	read(io, Uint8, 12) # reserved
	mdx = bool(read(io, Uint8)) # 0x1C
	langId = read(io, Uint8) # 0x1D
	read(io, Uint8, 2) # reserved # 0x1E
	fields = Array(DBFFieldDescriptor, 0)

	while !eof(io)
		push!(fields, read_dbf_field(io))
		p = position(io)
		trm = read(io, Uint8)
		if trm == 0xD
			break
		else
			seek(io, p)
		end
	end

	return DBFHeader(ver, last_update, records, hsize, rsize,
					 incomplete, encrypted, mdx, langId,
					 fields)
end

function read_dbf_records!(io::IO, df::DataFrame, header::DBFHeader; deleted=false)
	rc = 0
	while header.records != rc
		is_deleted = (read(io, Uint8) == 0x2A)
		r = Any[]
		for i = 1:length(header.fields)
			#print("P: $(position(io)) ")
			fld_data = read(io, Uint8, header.fields[i].len)
			#println("D: $(ascii(fld_data))")
			if header.fields[i].typ == Bool
				logical = char(fld_data)[1]
				if logical in ['Y', 'y', 'T', 't']
					push!(r, true)
				elseif logical in ['N', 'n', 'F', 'f']
					push!(r, false)
				else
					push!(r, NA)
				end
			elseif header.fields[i].typ == Int
				push!(r, parseint(header.fields[i].typ, ascii(fld_data)))
			elseif header.fields[i].typ == Float64
				push!(r, parsefloat(header.fields[i].typ, ascii(fld_data)))
			elseif header.fields[i].typ == String
				push!(r, strip(ascii(fld_data)))
			elseif header.fields[i].typ == Nothing
				push!(r, NA)
			else
				warn("Type $(header.fields[i].typ) is not supported")
			end
		end
		if !is_deleted || deleted
			push!(df, r)
		end
		rc += 1
		#println("R: $(position(io)), $(eof(io)), $(rc) ")
	end
	return df
end

function read_dbf(io::IO; deleted=false)
    header = read_dbf_header(io)
	df = DataFrame(map(f->f.typ, header.fields), map(f->symbol(f.nam), header.fields), 0)
	read_dbf_records!(io, df, header; deleted=deleted)
	return df
end

function read_dbf(fnm::ASCIIString; deleted=false)
	io = open(fnm)
	df = read_dbf(io; deleted=deleted)
	close(io)
	return df
end
