# unit tests of extract_string
x = "12345678"
@assert DataFrames.extract_string(x, 3, 6) == "3456"
@assert DataFrames.extract_string(x, 3, 3) == "3"
@assert DataFrames.extract_string(x, 3, 6, Set(3)) == "456"
@assert DataFrames.extract_string(x, 3, 6, Set(5, 3)) == "46"
x = "\"Güerín\",\"Sí\",\"No\""
@assert DataFrames.extract_string(x, 2, 7, Set(3)) == "Gerí"
@assert DataFrames.extract_string("", 0,0,Set()) == ""

test1 = IOString("I'm A,I'm B,I'm C,-0.3932755625236671,20.157657978753534")
res1 = DataFrames.read_separated_line(test1, ',', '"')
@assert res1[2] == "I'm B"
@assert res1[4] == "-0.3932755625236671"

test2 = IOString("123, 456 , \"789\",TRUE")
res2 = DataFrames.read_separated_line(test2, ',', '"')
@assert res2[2] == "456"
@assert res2[3] == "789"

test3 = IOString("123 ,456 , \"789\" , \"TRUE\"")
res3 = DataFrames.read_separated_line(test3, ',', '"')
@assert res3[2] == "456"
@assert res3[4] == "TRUE"

test4 = IOString("123 ,456 ,  , \"TRUE\"")
res4 = DataFrames.read_separated_line(test4, ',', '"')
@assert res4[3] == ""
@assert res4[4] == "TRUE"

test5 = IOString("123 ,456 , \"a\"\"b\" ,\"TRUE\"")
res5 = DataFrames.read_separated_line(test5, ',', '"')
@assert res5[3] == "a\"b"
@assert res5[4] == "TRUE"

test6 = IOString("123 ,456 , \"a
b\" ,\"TRUE\"")
res6 = DataFrames.read_separated_line(test6, ',', '"')
@assert res6[3] == "a\nb"
@assert res6[4] == "TRUE"

test7 = IOString("")
res7 = DataFrames.read_separated_line(test7, ',', '"')
@assert length(res7) == 1

test8 = IOString("a,\"b\",\"cd\",1.0,1\na,\"b\",\"cd\",1.0,1")
res8 = DataFrames.read_separated_line(test8, ',', '"')
@assert length(res8) == 5
@assert res8[5] == "1"

test9 = IOString("\"Güerín\",\"Sí\",\"No\"")
res9 = DataFrames.read_separated_line(test9, ',', '"')
@assert res9[2] == "Sí"

test10 = IOString("1,2,3,,")
res10 = DataFrames.read_separated_line(test10, ',', '"')
@assert length(res10) == 5

filename = file_path(julia_pkgdir(),"DataFrames/test/data/simple_data.csv")
open(filename,"r") do io
    t1 = read_table(io, ',', '"', DataFrames.DEFAULT_MISSINGNESS_INDICATORS, false, ["1","2","3","4","5"], 2)
    @assert nrow(t1) == 2
    @assert t1[1,2] == "b"
end



# Test separated line splitting
#
# TODO: Test minimially-quoted
# TODO: Test only-strings-quoted

separators = [',', '\t', ' ']
quotation_characters = ['\'', '"']

# Test all-entries-quoted for all quote characters and separators
items = {"a", "b", "c,d", "1.0", "1"}
item_buffer = Array(UTF8String, length(items))

# TODO: make this work with new splitting code
# for separator in separators
#   for quotation_character in quotation_characters
#     line = join(map(x -> strcat(quotation_character, x, quotation_character),
#                     items),
#                 separator)
#     current_item_buffer = Array(Char, strlen(line))
#     split_results = DataFrames.split_separated_line(line, separator, quotation_character, item_buffer, current_item_buffer)
#     @assert all(split_results .== items)
#   end
# end

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
column_names = UTF8String["A", "B", "C", "D", "E"]
minibatch_size = 10

file = open(filename, "r")
readline(file)
minibatch = read_minibatch(file,
	                       separator,
	                       quotation_character,
	                       missingness_indicators,
	                       column_names,
	                       minibatch_size)
@assert nrow(minibatch) == minibatch_size
@assert ncol(minibatch) == length(column_names)
@assert colnames(minibatch) == column_names
@assert eltype(minibatch[:, 1]) == UTF8String
@assert eltype(minibatch[:, 2]) == UTF8String
@assert eltype(minibatch[:, 3]) == UTF8String
@assert eltype(minibatch[:, 4]) == Float64
@assert eltype(minibatch[:, 5]) == Float64
close(file)

@elapsed df = read_table(filename)
@assert nrow(df) == 10_000
@assert ncol(df) == 5
@assert colnames(df) == column_names
@assert typeof(df[:, 1]) == DataVector{UTF8String}
@assert typeof(df[:, 2]) == DataVector{UTF8String}
@assert typeof(df[:, 3]) == DataVector{UTF8String}
@assert typeof(df[:, 4]) == DataVector{Float64}
@assert typeof(df[:, 5]) == DataVector{Float64}

# TODO: Split apart methods that perform seek() from those that don't
text_data = convert(Array{UTF8String, 2}, (["1" "3" "A"; "2" "3" "NA"; "3" "3.1" "C"]))

true_df = DataFrame(quote
                      x1 = DataArray([1, 2, 3])
                      x2 = DataArray([3, 3, 3.1])
                      x3 = DataArray(UTF8String["A", "", "C"], [false, true, false])
                    end)
df = DataFrames.convert_to_dataframe(text_data,
                                     ["", "NA"],
                                     ["x1", "x2", "x3"])
@assert isequal(df, true_df)
@assert isequal(eltype(df["x1"]), Int64)
@assert isequal(eltype(df["x2"]), Float64)
@assert isequal(eltype(df["x3"]), UTF8String)

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

# Additional data sets

@elapsed df = read_table("test/data/big_data.csv")
# TODO: Make this fast enough to include in testing
#@elapsed df = read_table("test/data/movies.csv")
# TODO: Release this data set publicly
#@elapsed df = read_table("test/data/bigrams.tsv")
@elapsed df = read_table("test/data/utf8.csv")
@elapsed df = read_table("test/data/bool.csv")
@elapsed df = read_table("test/data/types.csv")
@elapsed df = read_table("test/data/space_after_delimiter.csv")
@elapsed df = read_table("test/data/space_before_delimiter.csv")
@elapsed df = read_table("test/data/space_around_delimiter.csv")
@elapsed df = read_table("test/data/corrupt_utf8.csv")
