
* Workflow demo:
    * `csvDataFrame()` loading
    * Text summaries
    * referencing (basic, advanced)
    * assign
    * `with()` / `within()`
    * Split-Apply-Combine
    * Join/merge
    * Visualization
    * Linear regression modeling.

* Design/lower-level demo:
    * type hierarchy
    * DataVecs and NAs
    * PooledDataVecs
    * DataFrames
    * Indexes for column names
    * rbind, cbind
    * based_on?
    * subDataFrame, groupedDataFrame
    * grouping operations
    * pipelining
    * by, colwise
    * stack/unstack
    
    

* Extend Formula to include:
  * Interactions: `z ~ x:y` and `z ~ x*y`
  * Simple functions of variables: `y ~ log(x)`
  * Removing intercept: `y ~ x - 1`
  * Only intercept: `x ~ 1`
