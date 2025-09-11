# Guidelines

These are the general guidelines to use when generating code with Junie.

Be short when writing documentation. Do not explain trivial parameters, for example,
when a function has one `file` parameter, it does not need to be explained, it is obvious.

Keep functions short (10-15 lines) and well structured. If longer would be needed, separate
logically into multiple functions. Splitting a function into smaller functions makes it easier 
to understand and maintain.

Do not hide errors by returning some arbitrary default values. If not instructed otherwise,
use `Result` whenever a function can fail.

If not instructed otherise, propagate errors with `?`.