# Processing Streaming Data

In modern data analysis settings, we often need to work with streaming data
sources. This is particularly important when:

* Data sets are too large to store in RAM
* Data sets are being generated in real time

Julia is well-suited to both. The DataFrames package handles streaming
data by construcing a `DataStream`, which is an iterable object that
returns `DataFrame` one-by-one in small minibatches. By default, the
minibatches are single rows of data, but this can be easily changed. To
see how a `DataStream` works, it's easiest to convert an existing `DataFrame`
into a `DataStream` using the `DataStream` function:

    using DataFrames
    using RDatasets

    iris = data("datasets", "iris")

    iris = DataStream(iris)

We can then iterate over this stream of data using a standard `for` loop:

    for minibatch in iris
    	print_table(minibatch)
    end

## Streaming Large Scale Data Sets

COMING SOON

## Real Time Data Analysis

Another important case in which data must be dealt with using a streaming data
type comes up in real-time data analysis, when new data is constantly being
generated and an existing analysis needs to be updated as soon as possible.

In Julia, this can be addressed by piping new data into Julia using standard
UNIX pipes. To see how to work with data that comes in from a UNIX pipe,
copy the  following code into a program called streaming.jl:

	using DataFrames

	ds = DataStream(STDIN, 2)
	
	for df in ds
	  println("==================  MINIBATCH  ==================")
	  print_table(df)
	  print("\n\n\n")
	end

Now call this program from a UNIX terminal with a command like:

	cat ~/.julia/DataFrames/test/data/bool.csv | julia streaming.jl

Once that's done, sit back and watch how minibatches of data come streaming
in. Because the reader infers column names and types on the fly, you only
need to tell the reader what size the minibatches of data that you want to
process should be. You can then write simple for loops to process the
incoming data stream. You even do this by typing data into `STDIN`: just type
`julia streaming.jl`, then enter a data set line-by-line and hit `CTRL-D`.
