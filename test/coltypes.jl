Base.Test
using DataFrames

let
    # Readtable defining column types
    # How to test UTF8String ?
    filename = "data/factors/mixedvartypes2.csv"

    df = readtable(filename)
    @assert typeof(df["n"]) == DataArray{Int64,1}
    @assert df["n"][1] == 1
    @assert typeof(df["s"]) == DataArray{UTF8String,1}
    @assert df["s"][1] == "text"
    @assert typeof(df["f"]) == DataArray{Float64,1}
    @assert df["f"][1] == 2.3
    @assert typeof(df["b"]) == DataArray{Bool,1}
    @assert df["b"][1] == true

    df = readtable(filename,coltypes={"Int64", "UTF8String", "Float64", "Bool"})
    @assert typeof(df["n"]) == DataArray{Int64,1}
    @assert df["n"][1] == 1
    @assert typeof(df["s"]) == DataArray{UTF8String,1}
    @assert df["s"][1] == "text"
    @assert df["s"][4] == "text ole"
    @assert typeof(df["f"]) == DataArray{Float64,1}
    @assert df["f"][1] == 2.3
    @assert typeof(df["b"]) == DataArray{Bool,1}
    @assert df["b"][1] == true

    df = readtable(filename,coltypes={"Float64", "ASCIIString", "Int64", "ASCIIString"})
    @assert typeof(df["n"]) == DataArray{Float64,1}
    @assert df["n"][1] == 1.0
    @assert typeof(df["s"]) == DataArray{ASCIIString,1}
    @assert df["s"][2] == "more text"
    @assert isna(df["s"][3])
    @assert typeof(df["f"]) == DataArray{Int64,1}
    @assert df["f"][1] == 2
    @assert df["f"][2] == 0
    @assert df["f"][3] == 6
    @assert typeof(df["b"]) == DataArray{ASCIIString,1}
    @assert df["b"][1] == "true"
    @assert df["b"][2] == "false"

    df = readtable(filename,coltypes={"UTF8String", "Bool", "UTF8String", "Float64"})
    @assert typeof(df["n"]) == DataArray{UTF8String,1}
    @assert df["n"][4] == "57"
    @assert typeof(df["s"]) == DataArray{Bool,1}
    #@assert df["s"][2] == true
    #@assert df["s"][3] == false
    @assert typeof(df["f"]) == DataArray{UTF8String,1}
    @assert df["f"][1] == "2.3"
    @assert df["f"][4] == "2.010"
    @assert typeof(df["b"]) == DataArray{Float64,1}
    #@assert df["b"][1] == 1.0
    @assert df["b"][2] == 0.0

    df = readtable(filename,coltypes={"Bool", "UTF8String", "Bool", "Int64"})
    @assert typeof(df["n"]) == DataArray{Bool,1}
    #@assert df["n"][1] == true
    @assert df["n"][2] == false
    #@assert df["n"][4] == true
    @assert typeof(df["f"]) == DataArray{Bool,1}
    #@assert df["f"][1] == true
    @assert df["f"][5] == false
    @assert typeof(df["b"]) == DataArray{Int64,1}
    #@assert df["b"][1] == 1
    @assert df["b"][2] == 0
end
