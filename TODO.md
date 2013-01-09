Formulas
Parsing
Model matrix
Get dummies
Factors

Change unique() to return present elements, while levels() returns dead elements as well
cut(dv::DataVector)

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
    * DataVector's and NAs
    * PooledDataVector's
    * DataFrame's
    * Indexes for column names
    * rbind, cbind
    * based_on?
    * subDataFrame, groupedDataFrame
    * grouping operations
    * pipelining
    * by, colwise
    * stack/unstack

* Bugs
    * expand doesn't work with PooledDataVector's with pool of length 1
