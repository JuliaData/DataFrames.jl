Change unique() to return present elements, while levels() returns dead elements as well

* Workflow demo:
    * `csvDataFrame()` loading
    * Text summaries
    * referencing (basic, advanced)
    * assign
    * `with()` / `within()`
    * Split-Apply-Combine
    * Join/merge
    * Visualization
    * (Generalized) linear regression modeling using formulas

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
    
* Bugs
    * expand doesn't work with PooledDataVecs with pool of length 1
