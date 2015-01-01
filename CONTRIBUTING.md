Thanks for taking the plunge!

## Reporting Issues

* It's always good to start with a quick search for an existing issue to post on, or related issues for context, before opening a new issue
* Including minimal examples is greatly appeciated
* If it's a bug, or unexpected behaviour, reproducing on the latest development version (`Pkg.checkout("DataFrames")`) is a good gut check and can streamline the process, along with including the first two lines of output from `versioninfo()`

## Contributing

* Feel free to open, or comment on, an issue and solicit feedback early on, especially if you're unsure about aligning with design goals and direction, or if relevant historical comments are ambiguous
* Pair new functionality with tests, and bug fixes with tests that fail pre-fix. Increasing test coverage as you go is always nice
* Aim for atomic commits, if possible, e.g. `change 'foo' behavior like so` & `'bar' handles such and such corner case`, rather than `update 'foo' and 'bar'` & `fix typo` & `fix 'bar' better`
* Pull requests are tested against release and development branches of Julia, so using `Pkg.test("DataFrames")` as you develop can be helpful
* The style guidelines outlined below are not the personal style of most contributors, but for consistency throughout the project, we've adopted them

## Style Guidelines

* Include spaces
    + After commas
    + Around operators: `=`, `<:`, comparison operators, and generally around others
    + But not after opening parentheses or before closing parentheses
* Use four spaces for indentation (test data files and Makefiles excepted)
* Don't leave trailing whitespace at the end of lines
* Don't go over the 79 per-line character limit
* Avoid squashing code blocks onto one line, e.g. `for foo in bar; baz += qux(foo); end`
* Don't explicitly parameterize types unless it's necessary
* Never leave things without type qualifications. Use an explicit `::Any`.
* Order method definitions from most specific to least specific type constraints
