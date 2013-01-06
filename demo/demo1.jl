# Load DataFrame package
require("DataFrames")
using DataFrames

# Load a CSV file into a DataFrame
df = read_table("demo/toy_example.csv")

# Basic indexing
df[1, :]
df["A"]
df[1, "A"]
df[1:2, "A"]
df[1:2, ["A", "B"]]

# Use the with() function to evaluate expressions relative to a DataFrame
with(df, :(A + C))
