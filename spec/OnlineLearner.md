An OnlineLearner encapsulates the online analysis of data. It does so by implementing a protocol defined by the following behaviors:

* `state(learner::OnlineLearner)`: Reports the learner's state of knowlege
* `update(learner::OnlineLearner, ds::DataFrame)`: 
* `learn(learner::OnlineLearner, ds::DataStream)`: 

The state of knowledge of an OnlineLearner is arbitrary, so it has type `Any` in Julia. In the examples I am interested, the state of knowledge will always be a DataFrame, but that is not required. For example, for online Bayesian data analysis, the state of knowledge would be a probability distribution.

I am working to implement the following basic demonstration of an OnlineLearner:

* Given a DataStream, find the mean of each column.

This requires quite a lot of work. For example, we need to:

* Define a function that calculates the mean of an arbitrary column from a DataFrame.
* Define a function that calculates the mean of a DataFrame by building up a DataFrame out of the means of each column calculated separately.
* Define a method for properly calculating the weighted average of the estimated mean of one DataFrame with the estimated mean of another DataFrame, where there second DataFrame will be a minibatch provided to us by a DataStream.
* Wrap the values calculated over all minibatches in a simple Julia function.
