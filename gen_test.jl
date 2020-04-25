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

genby(df, :id, :b => mean)

keep(df, [:b, :c] => +)
keep(df, [:b] => mean)

keepby(df, :id, :b => mean)

collapse(df, [:b, :c] => +)
collapse(df, [:b] => mean)

collapseby(df, :id, :b => mean)

agggen(gd, [:b, :c] => +)
agggen(gd, :b => mean)

aggkeep(gd, [:b, :c] => +)
aggkeep(gd, :b => mean)

aggcollapse(gd, [:b, :c] => +)
aggcollapse(gd, :b => mean)


