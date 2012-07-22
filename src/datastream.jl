#
# Code for *SV types can be more abstract.
# Could add intermediate types.
# Could use macros.
#

#
# Temporary hack to work with CSV, TSV and WSV files.
#

function parse_csv_line(line)
  map(x -> float(x), split(line, ","))
end

function parse_tsv_line(line)
  map(x -> float(x), split(line, "\t"))
end

function parse_wsv_line(line)
  map(x -> float(x), split(line, r"\s+"))
end

#
# Abstract Type
#

abstract DataStream

#
# Concrete Types
#

type MatrixDataStream <: DataStream
  matrix::Matrix
  minibatch_size::Int
end

type CSVDataStream <: DataStream
  filename::String
  stream::IOStream
  minibatch_size::Int
end

type TSVDataStream <: DataStream
  filename::String
  stream::IOStream
  minibatch_size::Int
end

type WSVDataStream <: DataStream
  filename::String
  stream::IOStream
  minibatch_size::Int
end

type SQLDataStream <: DataStream
  minibatch_size::Int
end

#
# Functions
#

function MatrixDataStream(m::Matrix)
  MatrixDataStream(m, 1)
end

function start(mds::MatrixDataStream)
  1
end

function next(mds::MatrixDataStream, index::Int)
  index_set = index:(index + mds.minibatch_size - 1)
  # May attept to index past boundary. Need to handle properly.
  (mds.matrix[index_set , :], index + mds.minibatch_size)
end

function done(mds::MatrixDataStream, index::Int)
  index > size(mds.matrix, 1)
end

#
# CSV
#

function CSVDataStream(filename::String, minibatch_size::Int)
  CSVDataStream(filename, open(filename, "r"), minibatch_size)
end

function CSVDataStream(filename::String)
  CSVDataStream(filename, open(filename, "r"), 1)
end

function start(cds::CSVDataStream)
  cds.stream = open(cds.filename, "r")
  seek(cds.stream, 0)
  readline(cds.stream)
end

function next(cds::CSVDataStream, line::String)
  (parse_csv_line(chomp(line)), readline(cds.stream))
end

function done(cds::CSVDataStream, line::String)
  if !isempty(line)
    return false
  else
    close(cds.stream)
    return true
  end
end

#
# TSV
#

function TSVDataStream(filename::String, minibatch_size::Int)
  TSVDataStream(filename, open(filename, "r"), minibatch_size)
end

function TSVDataStream(filename::String)
  TSVDataStream(filename, open(filename, "r"), 1)
end

function start(tds::TSVDataStream)
  tds.stream = open(tds.filename, "r")
  seek(tds.stream, 0)
  readline(tds.stream)
end

function next(tds::TSVDataStream, line::String)
  (parse_tsv_line(chomp(line)), readline(tds.stream))
end

function done(tds::TSVDataStream, line::String)
  if !isempty(line)
    return false
  else
    close(tds.stream)
    return true
  end
end

#
# WSV
#

function WSVDataStream(filename::String, minibatch_size::Int)
  WSVDataStream(filename, open(filename, "r"), minibatch_size)
end

function WSVDataStream(filename::String)
  WSVDataStream(filename, open(filename, "r"), 1)
end

function start(wds::WSVDataStream)
  wds.stream = open(wds.filename, "r")
  seek(wds.stream, 0)
  readline(wds.stream)
end

function next(wds::WSVDataStream, line::String)
  (parse_wsv_line(chomp(line)), readline(wds.stream))
end

function done(wds::WSVDataStream, line::String)
  if !isempty(line)
    return false
  else
    close(wds.stream)
    return true
  end
end
