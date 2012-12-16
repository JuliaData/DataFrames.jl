require("extras/test.jl")

load("DataFrames")
using DataFrames

# Test separated line splitting
#
# TODO: Test minimially-quoted
# TODO: Test only-strings-quoted

separators = [',', '\t', ' ']
quotation_characters = ['\'', '"']

# Test all-entries-quoted for all quote characters and separators
items = {"a", "b", "c,d", "1.0", "1"}
item_buffer = Array(UTF8String, length(items))

for separator in separators
  for quotation_character in quotation_characters
    line = join(map(x -> strcat(quotation_character, x, quotation_character),
                    items),
                separator)
    current_item_buffer = Array(Uint8, strlen(line))
    split_results = DataFrames.split_separated_line(line, separator, quotation_character, item_buffer, current_item_buffer)
    @assert all(split_results .== items)
  end
end

# Test type inference heuristic

@assert DataFrames.int_able("1234") == true
@assert DataFrames.int_able("-1234") == true
@assert DataFrames.int_able("1234.13") == false
@assert DataFrames.int_able("-1234.13") == false
@assert DataFrames.int_able("1.13e1") == false
@assert DataFrames.int_able("-1.13e1") == false
@assert DataFrames.int_able("1234a") == false
@assert DataFrames.int_able("blah") == false

@assert DataFrames.float_able("1234") == true
@assert DataFrames.float_able("-1234") == true
@assert DataFrames.float_able("1234.13") == true
@assert DataFrames.float_able("-1234.13") == true
@assert DataFrames.float_able("1.13e1") == true
@assert DataFrames.float_able("-1.13e1") == true
@assert DataFrames.float_able("1234a") == false
@assert DataFrames.float_able("blah") == false

import DataFrames.INT64TYPE
import DataFrames.FLOAT64TYPE
import DataFrames.UTF8TYPE

@assert DataFrames.tightest_type("1234", INT64TYPE) == INT64TYPE
@assert DataFrames.tightest_type("-1234", INT64TYPE) == INT64TYPE
@assert DataFrames.tightest_type("1234.2", INT64TYPE) == FLOAT64TYPE
@assert DataFrames.tightest_type("-1234.2", INT64TYPE) == FLOAT64TYPE
@assert DataFrames.tightest_type("1234AFX", INT64TYPE) == UTF8TYPE
@assert DataFrames.tightest_type("-1234AFX", INT64TYPE) == UTF8TYPE

@assert DataFrames.tightest_type("1234", FLOAT64TYPE) == FLOAT64TYPE
@assert DataFrames.tightest_type("-1234", FLOAT64TYPE) == FLOAT64TYPE
@assert DataFrames.tightest_type("1234.2", FLOAT64TYPE) == FLOAT64TYPE
@assert DataFrames.tightest_type("-1234.2", FLOAT64TYPE) == FLOAT64TYPE
@assert DataFrames.tightest_type("1234AFX", FLOAT64TYPE) == UTF8TYPE
@assert DataFrames.tightest_type("-1234AFX", FLOAT64TYPE) == UTF8TYPE

@assert DataFrames.tightest_type("1234", UTF8TYPE) == UTF8TYPE
@assert DataFrames.tightest_type("-1234", UTF8TYPE) == UTF8TYPE
@assert DataFrames.tightest_type("1234.2", UTF8TYPE) == UTF8TYPE
@assert DataFrames.tightest_type("-1234.2", UTF8TYPE) == UTF8TYPE
@assert DataFrames.tightest_type("1234AFX", UTF8TYPE) == UTF8TYPE
@assert DataFrames.tightest_type("-1234AFX", UTF8TYPE) == UTF8TYPE

filename = file_path(julia_pkgdir(),"DataFrames/test/data/big_data.csv")
separator = ','
quotation_symbol = '"'
header = true
missingness_indicators = ["", "NA"]

(column_names, column_types, nrows) =
 DataFrames.determine_metadata(filename,
                               separator,
                               quotation_symbol,
                               missingness_indicators,
                               header,
                               false)
@assert column_names == ["A", "B", "C", "D", "E"]
@assert column_types == {UTF8String, UTF8String, UTF8String, Float64, Float64}

# From now on, files do not have headers
header = false

filename = file_path(julia_pkgdir(),"DataFrames/test/data/sample_data.csv")

(column_names, column_types) =
 DataFrames.determine_metadata(filename,
                               separator,
                               quotation_symbol,
                               missingness_indicators,
                               header,
                               false)
@assert column_names == ["x1", "x2", "x3"]
@assert column_types == {Int64, Int64, Int64}

filename = file_path(julia_pkgdir(),"DataFrames/test/data/simple_data.csv")

(column_names, column_types) =
 DataFrames.determine_metadata(filename,
                               separator,
                               quotation_symbol,
                               missingness_indicators,
                               header,
                               false)
@assert column_names == ["x1", "x2", "x3", "x4", "x5"]
@assert column_types == {UTF8String, UTF8String, UTF8String, Float64, Int64}

filename = file_path(julia_pkgdir(),"DataFrames/test/data/complex_data.csv")

(column_names, column_types) =
 DataFrames.determine_metadata(filename,
                               separator,
                               quotation_symbol,
                               missingness_indicators,
                               header,
                               false)
@assert column_names == ["x1", "x2", "x3", "x4", "x5"]
@assert column_types == {UTF8String, UTF8String, UTF8String, Float64, Int64}

# Test reading
@assert DataFrames.determine_separator("blah.csv") == ','
@assert DataFrames.determine_separator("blah.tsv") == '\t'
@assert DataFrames.determine_separator("blah.wsv") == ' '
# @assert DataFrames.determine_separator("blah.txt")
# Need to change to use @expects to test that error gets raised

filename = file_path(julia_pkgdir(),"DataFrames/test/data/big_data.csv")
separator = DataFrames.determine_separator(filename)
quotation_character = '"'
missingness_indicators = ["", "NA"]
header = true
column_names, column_types = DataFrames.determine_metadata(filename,
	                                                       separator,
	                                                       quotation_character,
	                                                       missingness_indicators,
	                                                       header,
                                                         false)
minibatch_size = 10

file = open(filename, "r")
readline(file)
minibatch = read_minibatch(file,
	                       separator,
	                       quotation_character,
	                       missingness_indicators,
	                       column_names,
	                       column_types,
	                       minibatch_size)
@assert nrow(minibatch) == minibatch_size
@assert ncol(minibatch) == length(column_names)
@assert colnames(minibatch) == column_names
@assert typeof(minibatch[:, 1]) == DataVec{column_types[1]}
@assert typeof(minibatch[:, 2]) == DataVec{column_types[2]}
@assert typeof(minibatch[:, 3]) == DataVec{column_types[3]}
@assert typeof(minibatch[:, 4]) == DataVec{column_types[4]}
@assert typeof(minibatch[:, 5]) == DataVec{column_types[5]}
close(file)

@elapsed df = read_table(filename)
@elapsed md = DataFrames.determine_metadata(filename, ',', '"', ["", "NA"], true, false)
@assert nrow(df) == 10_000
@assert ncol(df) == 5
@assert colnames(df) == column_names
@assert typeof(df[:, 1]) == DataVec{column_types[1]}
@assert typeof(df[:, 2]) == DataVec{column_types[2]}
@assert typeof(df[:, 3]) == DataVec{column_types[3]}
@assert typeof(df[:, 4]) == DataVec{column_types[4]}
@assert typeof(df[:, 5]) == DataVec{column_types[5]}

# Test UTF8 support
# TODO: Make this work in Julia's core
# df = read_table("test/data/utf8.csv")

# TODO: Split apart methods that perform seek() from those that don't
text_data = ["1" "3" "A"; "2" "3" "NA"; "3" "3.1" "C"]
inferred_types = DataFrames.infer_column_types(text_data, ["", "NA"])
@assert inferred_types == {Int64, Float64, UTF8String}

true_df = DataFrame(quote
                      x1 = DataVec[1, 2, 3]
                      x2 = DataVec[3, 3, 3.1]
                      x3 = DataVec["A", NA, "C"]
                    end)
df = DataFrames.convert_to_dataframe(text_data,
                                     ["", "NA"],
                                     {Int64, Float64, ASCIIString},
                                     ["x1", "x2", "x3"])
@assert isequal(df, true_df)
@assert isequal(eltype(df["x1"]), Int64)
df = DataFrames.convert_to_dataframe(text_data,
                                     ["", "NA"],
                                     {Int64, Float64, UTF8String},
                                     ["x1", "x2", "x3"])
@assert isequal(eltype(df["x3"]), UTF8String)
df = DataFrames.convert_to_dataframe(text_data,
                                     ["", "NA"],
                                     {Float64, Float64, UTF8String},
                                     ["x1", "x2", "x3"])
@assert isequal(eltype(df["x1"]), Float64)
df = DataFrames.convert_to_dataframe(text_data,
                                     ["", "NA"],
                                     {UTF8String, Float64, UTF8String},
                                     ["x1", "x2", "x3"])
@assert isequal(eltype(df["x1"]), UTF8String)

filename = file_path(julia_pkgdir(),"DataFrames/test/data/big_data.csv")
separator = DataFrames.determine_separator(filename)
quotation_character = '"'
missingness_indicators = ["", "NA"]
header = true

nrows = DataFrames.determine_nrows(filename, header)
@assert nrows == 10_000
ncols = DataFrames.determine_ncols(filename, separator, quotation_character)
@assert ncols == 5

io = open(filename, "r")

column_names = DataFrames.determine_column_names(io, separator, quotation_character, header)
@assert column_names == UTF8String["A", "B", "C", "D", "E"]

seek(io, 0)
if header
  readline(io)
end
text_data = DataFrames.read_separated_text(io, nrows, ncols, separator, quotation_character)
@assert eltype(text_data) == UTF8String
@assert size(text_data) == (10_000, 5)

df = read_table(io,
                separator,
                quotation_character,
                missingness_indicators,
                header,
                column_names,
                nrows)
@assert nrow(df) == 10_000
@assert ncol(df) == 5
@assert colnames(df) == column_names
@assert eltype(df[:, 1]) == UTF8String
@assert eltype(df[:, 2]) == UTF8String
@assert eltype(df[:, 3]) == UTF8String
@assert eltype(df[:, 4]) == Float64
@assert eltype(df[:, 5]) == Float64

df = read_table(filename)
@assert nrow(df) == 10_000
@assert ncol(df) == 5
@assert colnames(df) == column_names
@assert eltype(df[:, 1]) == UTF8String
@assert eltype(df[:, 2]) == UTF8String
@assert eltype(df[:, 3]) == UTF8String
@assert eltype(df[:, 4]) == Float64
@assert eltype(df[:, 5]) == Float64

# TODO: Add test case in which data file has header, but no rows
# Example "RDatasets/data/Zelig/sna.ex.csv"
# "","Var1","Var2","Var3","Var4","Var5"
