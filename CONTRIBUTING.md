[![ColPrac: Contributor's Guide on Collaborative Practices for Community Packages](https://img.shields.io/badge/ColPrac-Contributor's%20Guide-blueviolet)](https://github.com/SciML/ColPrac)

We follow the ColPrac guide for collaborative practices. New contributor should make sure to read that guide.
Following here are some additional clarifications and practices we follow.

Thanks for taking the plunge!

## Reporting Issues

* It's always good to start with a quick search for an existing issue to post on,
  or related issues for context, before opening a new issue
* Including minimal examples is greatly appreciated
* If it's a bug, or unexpected behaviour, reproducing on the latest development version
  (`Pkg.add(name="DataFrames", rev="main")`) is a good gut check and can streamline the process,
  along with including the first two lines of output from `versioninfo()`

## Modifying an existing docstring in `src/`

All docstrings are written inline above the methods or types they are associated with and can
be found by clicking on the `source` link that appears below each docstring in the documentation.
The steps needed to make a change to an existing docstring are listed below:

* Create a new branch;
* Find the docstring in `src/`;
* Update the text in the docstring;
* run `julia make.jl` from the `/docs` directory;
* check the output in `doc/_build/html/` to make sure the changes are correct;
* commit your changes and open a pull request.
* a preferred structure of docstring for a function is:
  + list of accepted signatures;
  + a short information what function does;
  + description of its arguments;
  + additional details if needed;
  + examples.

## Adding a new docstring to `src/`

The steps required to add a new docstring are listed below:
* find a suitable object definition in `src/` that the docstring
  will be most applicable to;
* add a doctring above the definition;
* find a suitable `@docs` code block in the `docs/src/lib/functions.md` file 
  where you would like the docstring to appear; (if the docstring is added
  to an object that is not exported add it to `docs/src/lib/internals.md`);
* add the name of the definition to the `@docs` code block. For example, with a docstring
  added to a function `bar`.
```
"..."
function bar(args...)
    # ...
end
```

you would add the name `bar` to a `@docs` block in `docs/src/lib/functions.md`
````
```@docs
foo
bar # <-- Added this one.
baz
```
````

* run `julia make.jl` from the `/docs` directory;
* check the output in `docs/_build/html` to make sure the changes are correct;
* commit your changes and open a pull request.

## Doctests

Examples written within docstrings can be used as test cases known as *doctests* by annotating code 
blocks with `jldoctest`:
````
```jldoctest
julia> uppercase("Docstring test")
"DOCSTRING TEST"
```
````
A doctest needs to match an interactive REPL including the `julia>` prompt. To run 
doctests run `julia make.jl` from the `/docs` directory.

It is recommended to add the header `# Examples` above the doctests.

## Contributing

* DataFrames.jl is a relatively complex package that also has many external dependencies.
  Therefore if you would want to propose a new functionality (which is encouraged) it is
  strongly recommended to open an issue first and reach a decision on the final design.
  Then a pull request serves an implementation of the agreed way how things should work.
* If you are a new contributor and would like to get a guidance on what area
  you could focus your first PR please do not hesitate to ask and JuliaData members
  will help you with picking a topic matching your experience.
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
  ecosystem wait for such CI jobs to finish as they share a common pool of CI resources.

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
* Always include a digit after decimal when writing a float, e.g. `[1.0, 2.0]`
  rather than `[1., 2.]`
* In docstrings, optional arguments, including separators and spaces, are surrounded by brackets,
  e.g. `mymethod(required[, optional1[, optional2] ]; kwargs...)`

## Git Recommendations For Pull Requests

* Avoid working from the `main` branch of your fork, creating a new branch will make it
  easier if DataFrames.jl `main` branch changes and you need to update your pull request;
* All PRs and issues should be opened against the `main` branch not against the current release;
* Run tests of your code before sending any commit to GitHub. Only push changes when 
  the tests of the change are passing locally. In particular note that it is not a problem
  if you send several commits in one push command to GitHub as CI will be run only once then;
* If any conflicts arise due to changes in DataFrames.jl `main` branch, prefer updating your pull
  request branch with `git rebase` (rather than `git merge`), since the latter will introduce a merge 
  commit that might confuse GitHub when displaying the diff of your PR, which makes your changes more 
  difficult to review. Alternatively use conflict resolution tool available at GitHub;
* Please try to use descriptive commit messages to simplify the review process;
* Using `git add -p` or `git add -i` can be useful to avoid accidentally committing unrelated changes;
* Maintainers get notified of all changes made on GitHub. However, what is useful is writing a short
  message after a sequence of changes is made summarizing what has changed and that the PR is ready
  for a review;
* When linking to specific lines of code in discussion of an issue or pull request, hit the `y` key
  while viewing code on GitHub to reload the page with a URL that includes the specific commit that 
  you're viewing. That way any lines of code that you refer to will still be correct in the future, even 
  if additional commits are pushed to the branch you are reviewing;
* Please make sure you follow the code formatting guidelines when submitting a PR;
  Also preferably do not modify parts of code that you are not editing as this makes
  reviewing the PR harder (it is better to open a separate maintenance PR
  if e.g. code layout can be improved);
* If a PR is not finished yet and should not be reviewed yet then it should be opened as DRAFT 
  (in this way maintainers will know that they can ignore such PR until it is made non-draft or the author
  asks for a review).
