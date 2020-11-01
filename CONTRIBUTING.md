[![ColPrac: Contributor's Guide on Collaborative Practices for Community Packages](https://img.shields.io/badge/ColPrac-Contributor's%20Guide-blueviolet)](https://github.com/SciML/ColPrac)

We follow the ColPrac guide for collaborative practices. New contributor should make sure to read that guide.
Following here are some additional clarifications and practices we follow.

Thanks for taking the plunge!

## Reporting Issues

* It's always good to start with a quick search for an existing issue to post on,
  or related issues for context, before opening a new issue
* Including minimal examples is greatly appreciated
* If it's a bug, or unexpected behaviour, reproducing on the latest development version
  (`Pkg.add(name="DataFrames", rev="master")`) is a good gut check and can streamline the process,
  along with including the first two lines of output from `versioninfo()`

## Contributing

* DataFrames.jl is a relatively complex package that also has many external dependencies.
  Therefore if you would want to propose a new functionality (which is encouraged) it is
  strongly recommended to open an issue first and reach a decision on the final design.
  Then a pull request serves an implementation of the agreed way how things should work.
* Feel free to open, or comment on, an issue and solicit feedback early on,
  especially if you're unsure about aligning with design goals and direction,
  or if relevant historical comments are ambiguous.
* Pair new functionality with tests, and bug fixes with tests that fail pre-fix.
  Increasing test coverage as you go is always nice.
* Aim for atomic commits, if possible, e.g. `change 'foo' behavior like so` &
  `'bar' handles such and such corner case`,
  rather than `update 'foo' and 'bar'` & `fix typo` & `fix 'bar' better`.
* Pull requests are tested against release and development branches of Julia,
  so using `Pkg.test("DataFrames")` as you develop can be helpful.
* The style guidelines outlined below are not the personal style of most contributors,
  but for consistency throughout the project, we've adopted them.
* It is recommended to disable GitHub Actions on your fork; check Settings > Actions.
* If a PR adds a new exported name then make sure to add a docstring for it and
  add a reference to it in the documentation.
* A PR with breaking changes should have `[BREAKING]` as a first part of its name.
* If a PR changes or adds functionality please update NEWS.md file accordingly as
  a part of the PR (along with the link to the PR); please do not add entries
  to NEWS.md for changes that are bug fixes or are not user visible, such as
  adding tests, updating documentation or improving code layout.
* If you make a PR please try to avoid pushing many small commits to GitHub in
  a sequence as each such commit triggers a separate CI job, which takes over
  an hour. This has a consequence of making other PRs in packages from the JuliaData
  ecosystem wait for such CI jobs to finish as hey share a common pool of CI resources.

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
