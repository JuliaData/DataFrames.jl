using DataFrames, CSV
german = CSV.read(
    joinpath(dirname(pathof(DataFrames)),
    "..", "docs", "src", "assets", "german.csv"),
    DataFrame
)

df = DataFrame(
    id = [1, 2, 3],
    first_name = ["José", "Emma", "Nathan"],
    last_name = ["Garcia", "Marino", "Boyer"],
    age = [61, 24, 33]
)
select(df, [:last_name, :first_name])
select(df, r"name")
select(df, Not(:id))
select(df, Between(2,4))

df = DataFrame(a = [1, 2, 3], b = [4, 5, 4])
combine(df, :a => sum)
transform(df, :b => maximum) # transform and select copy result to all rows
transform(df, [:b, :a] => -) # vector subtraction is okay
transform(df, [:a, :b] => *) # vector multiplication is not defined
transform(df, [:a, :b] => ByRow(*))
f(x) = x + 1
transform(df, :a => ByRow(f))
g(x) = x .+ 1
transform(df, :a => g)
select(df, :a => ByRow(x -> x + 1))
subset(df, :b => ByRow(x -> x < 5))
subset(df, :b => ByRow(<(5))) # shorter version
transform(df, [:a, :b] => ByRow((x, y) -> 2x + y))
transform(df, Cols(:) => ByRow(max))

# Want to show a multi-row dataframe example
df = DataFrame(a = 1:2, b = 3:4, c = 5:6, d = 2:-1:1)
select(df, Cols(:) => ByRow(min)) # `min` works on a multiple arguments
select(df, AsTable(:) => ByRow(minimum)) # `minimum` works on a collection

select(df, [:a,:b] => ByRow(+)) # `+` works on a multiple arguments
select(df, AsTable([:a,:b]) => ByRow(sum)) # `sum` works on a collection

using Statistics
select(df, AsTable(Between(:b, :d)) => ByRow(mean))
# sum and mean functions techically don't need ByRow, but it is probably good mental practice to use them.

f(nt) = nt.a + nt.d
transform(df, AsTable(:) => ByRow(f))

# Don't need to use. AsTable avoid the need for slurping and splatting.
f(columns...) = min.(columns...)
transform(df, Cols(:) => f)
g(columns...) = minimum.(columns)
transform(df, Cols(:) => g)
h(nt) = min.(nt...)
transform(df, AsTable(:) => h)
transform(df, Cols(:) => ByRow(min))

# new_column_names
df = DataFrame(a=1:4, b=5:8)
transform(df, :a => ByRow(x->x+10), renamecols=false)
transform(df, Cols(:) => ByRow(+), renamecols=false)
transform(df, Cols(:) => ByRow(+) => :c)
transform(df, Cols(:) => ByRow(+) => "a+b")
transform(df, :a => ByRow(x->x+10) => "a+10")
rename(df, :a => :α)

add_prefix(s) = "new_" * s
transform(df, :a => identity => add_prefix)

df = DataFrame(data = [(1,2), (3,4)]) # vector of tuples
transform(df, :data => [:first, :second]) # manual naming
transform(df, :data => AsTable) # default automatic naming with tuples
df = DataFrame(data = [(a=1,b=2), (a=3,b=4)]) # vector of named tuples
transform(df, :data => AsTable) # keeps names from named tuples

df = DataFrame(data = [(1,2), (3,4)])
new_names(v) = ["primary ", "secondary "] .* v
transform(df, :data => identity => new_names)

df = DataFrame(a = 1:4, b = [50,50,60,60], c = ["hat","bat","cat","dog"])
select(df, :c, :b, :a) # re-order columns
select(df, :b, :) # `:` here means all other columns
select(
    df,
    :c => (x -> "a " .* x) => :one_c,
    :a => (x -> 100x),
    :b,
    renamecols=false
) # can mix operation forms
combine(df, :a=>maximum, :b=>sum, :c=>join)
select(
    df,
    :c => ByRow(reverse),
    :c => ByRow(uppercase)
) # multiple operations on same column
subset(
    df,
    :b => ByRow(==(60)),
    :c => ByRow(contains("at"))
) # rows with 60 and "at"
transform(
    df,
    [:a, :b] => ByRow(+) => :d,
    :d => (x -> x ./ 2),
) # requires two separate transformations
new_df = transform(df, [:a, :b] => ByRow(+) => :d)
transform!(new_df, :d => (x -> x ./ 2) => :d_2)

typeof(:x => :a)
typeof("x" => "a")
typeof(1 => "a")

df = DataFrame(x = 1:3, y = 4:6)
select(df, :x => :a)
select(df, 1 => "a")

["x" => "a", "y" => "b"] == (["x", "y"] .=> ["a", "b"])
operation1 = ["x" => "a", "y" => "b"]
operation2 = ["x", "y"] .=> ["a", "b"]
operation1 == operation2
select(df, operation1)

df = DataFrame(x = 1:3, y = 4:6)
operation = ["x", "y"] .=> ["a", "b"]
typeof(operation)
first(operation)
last(operation)
select(df, operation)

f(x) = 2 * x
["x", "y"] .=> f .=> ["a", "b"]
select(df, ["x", "y"] .=> f .=> ["a", "b"])
newname(s::String) = s * "_new"
["x", "y"] .=> f .=> newname
select(df, ["x", "y"] .=> f .=> newname)

p = :x => :y => :z
p[1]
p[2]
p[2][1]
p[2][2]

df = DataFrame(
    Time = 1:4,
    Temperature1 = [20, 23, 25, 28],
    Temperature2 = [33, 37, 41, 44],
    Temperature3 = [15, 10, 4, 0],
)
celsius_to_kelvin(x) = x + 273
transform(df, Not("Time") .=> ByRow(celsius_to_kelvin), renamecols = false)
transform(df, 2:4 .=> ByRow(celsius_to_kelvin), renamecols = false)
transform(
    df,
    Cols(r"Temp") .=> ByRow(celsius_to_kelvin),
    renamecols = false
)
rename_function(s) = "Temperature $(last(s)) (°K)"
select(
    df,
    "Time",
    Cols(r"Temp") .=> ByRow(celsius_to_kelvin) .=> rename_function
)

df = DataFrame(a=1:4, b=5:8)
f1(x) = x .+ 1
f2(x) = x ./ 10
transform(df, [:a, :b] .=> [f1, f2])
transform(df, [:a => f1, :b => f2])
[:a, :b] .=> [f1 f2] # No comma `,` between f1 and f2
transform(df, [:a, :b] .=> [f1 f2]) # No comma `,` between f1 and f2
