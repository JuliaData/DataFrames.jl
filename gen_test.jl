using DataFrames, Statistics

df = DataFrame(
	id = rand(["a", "b", "c", "d"], 100),
	a = rand(100),
	b = 100 * randn(100),
	c = rand([100, 200, 300], 100)
)

gd = groupby(copy(df), :id)

gen(df, [:b, :c] => +)
gen(df, [:b] => mean)

gen(groupby(df, :id), :id, :b => mean)
gen(groupby(df, :id), :id, :b => mean; keepgroup = true)


keep(df, [:b, :c] => +)
keep(df, [:b] => mean)

keep(groupby(df, :id), :id, :b => mean)
keep(groupby(df, :id), :id, :b => mean; keepgroup = true)
keep(groupby(df, :id), :id, :b => mean; keepgroup = false, keepkeys = false)


collapse(df, [:b, :c] => +)
collapse(df, [:b] => mean)

collapse(groupby(df, :id), :id, :b => mean)
collapse(groupby(df, :id), :id, :b => mean; keepgroup = true)
collapse(groupby(df, :id), :id, :b => mean; keepgroup = false, keepkeys = false)



