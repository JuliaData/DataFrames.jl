# Specification of DataStream as an Abstract Protocol

A DataStream object allows one to abstractly write code that processes streaming data, which can be used for many things, including online analysis. Before we begin, we distinguish between streaming data and online analysis:

* Streaming data involves low memory usage access to a data source. Typically, one demands that a streaming data algorithm use much less memory than would be required to simply represent the full raw data source in main memory.
* Online analysis involves computations on data for which interim answers must be available. For example, given a list of a trillion numbers, one would like to have access to the estimated mean after seeing only the first _N_ elements of this list. Online estimation is essential for building practical statistical systems that will be deployed in the wild. Online analysis is the _sine qua non_ of active learning, in which a statistical system selects which data points to observe next.

In Julia, the DataStream protocol works as follows:

* A DataStream provides a connection to an immutable source of data that implements the following Iterator protocol found throughout Julia:
 * `start(iter)`: Get initial iteration state.
 * `next(iter, state)`: For a given iterable object and iteration state, return the current item and the next iteration state.
 * `done(iter, state)`: Test whether we are done iterating.
* Each call to `next()` causes the DataStream object to read in a chunk of rows of tabular data from the streaming source and store these in a DataFrame. This chunk is called a minibatch and its maximum size is specified at the time the DataStream is created. It defaults to _1_.
* All rows from the data source must use the same tabular schema. Entries may be missing, but this missingness must be represented explicitly by the DataStream.

We enforce this behavior and provide seamless access to streaming minibatches through an abstract `DataStream` type. In practice, we work with specific DataStream types that wrap access to specific data sources like CSV files or SQL databases, but algorithms can be designed to accept any DataStream by insuring that the algorithm only interacts with a DataStream through the specified protocol. For now, we have implemented the following:

* MatrixDataStream
* CSVDataStream
* TSVDataStream
* WSVDataStream

In the future, we will want to implement:

* SQLDataStream
* Other tabular data sources

Note that NoSQL databases are currently difficult to support because of the flexible schemas. We will need to think about how to interface with such systems in the future.

# Use Cases

We can compute many useful quantities using DataStream's:

* Means
* Variances
* Covariances
* Correlations
* Unique element lists and counts
* Linear models
* Entropy

# Advice on Deploying DataStreams

* Many useful computations in statistics can be done online:
  * Estimation of means, including implicit estimation of means in Reinforcement Learning
  * Estimation of entropy
  * Estimation of linear regression models
* But many other computations cannot be done online because they require completing a full pass through the data before quantities can be computed exactly.
* Before writing a DataStream algorith, ask yourself: "what is the performance of this algorithm if I only allow it to make one pass through the data?"

# References

* McGregor: Crash Course on Data Stream Algorithms
* Muthukrishnan : Data Streams - Algorithms and Applications
* Chakrabarti: CS85 - Data Stream Algorithms
* Knuth: Art of Computer Programming
