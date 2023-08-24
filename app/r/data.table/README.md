
# data.table <a href="https://r-datatable.com"><img src="https://raw.githubusercontent.com/Rdatatable/data.table/master/.graphics/logo.png" align="right" height="140" /></a>


`data.table` provides a high-performance version of [base R](https://www.r-project.org/about.html)'s `data.frame` with syntax and feature enhancements for ease of use, convenience and programming speed.

## Why `data.table`?

* concise syntax: fast to type, fast to read
* fast speed
* memory efficient
* careful API lifecycle management
* community
* feature rich

## Features

* fast and friendly delimited **file reader**: **[`?fread`](https://rdatatable.gitlab.io/data.table/reference/fread.html)**, see also [convenience features for _small_ data](https://github.com/Rdatatable/data.table/wiki/Convenience-features-of-fread)
* fast and feature rich delimited **file writer**: **[`?fwrite`](https://rdatatable.gitlab.io/data.table/reference/fwrite.html)**
* low-level **parallelism**: many common operations are internally parallelized to use multiple CPU threads
* fast and scalable aggregations; e.g. 100GB in RAM (see [benchmarks](https://h2oai.github.io/db-benchmark/) on up to **two billion rows**)
* fast and feature rich joins: **ordered joins** (e.g. rolling forwards, backwards, nearest and limited staleness), **[overlapping range joins](https://github.com/Rdatatable/data.table/wiki/talks/EARL2014_OverlapRangeJoin_Arun.pdf)** (similar to `IRanges::findOverlaps`), **[non-equi joins](https://github.com/Rdatatable/data.table/wiki/talks/ArunSrinivasanUseR2016.pdf)** (i.e. joins using operators `>, >=, <, <=`), **aggregate on join** (`by=.EACHI`), **update on join**
* fast add/update/delete columns **by reference** by group using no copies at all
* fast and feature rich **reshaping** data: **[`?dcast`](https://rdatatable.gitlab.io/data.table/reference/dcast.data.table.html)** (_pivot/wider/spread_) and **[`?melt`](https://rdatatable.gitlab.io/data.table/reference/melt.data.table.html)** (_unpivot/longer/gather_)
* **any R function from any R package** can be used in queries not just the subset of functions made available by a database backend, also columns of type `list` are supported
* has **[no dependencies](https://en.wikipedia.org/wiki/Dependency_hell)** at all other than base R itself, for simpler production/maintenance
* the R dependency is **as old as possible for as long as possible**, dated April 2014, and we continuously test against that version; e.g. v1.11.0 released on 5 May 2018 bumped the dependency up from 5 year old R 3.0.0 to 4 year old R 3.1.0



## Usage

Use `data.table` subset `[` operator the same way you would use `data.frame` one, but...

* no need to prefix each column with `DT$` (like `subset()` and `with()` but built-in)
* any R expression using any package is allowed in `j` argument, not just list of columns
* extra argument `by` to compute `j` expression by group

```r
library(data.table)
DT = as.data.table(iris)

# FROM[WHERE, SELECT, GROUP BY]
# DT  [i,     j,      by]

DT[Petal.Width > 1.0, mean(Petal.Length), by = Species]
#      Species       V1
#1: versicolor 4.362791
#2:  virginica 5.552000
```

### Getting started

* [Introduction to data.table](https://cran.r-project.org/package=data.table/vignettes/datatable-intro.html) vignette
* [Getting started](https://github.com/Rdatatable/data.table/wiki/Getting-started) wiki page
* [Examples](https://rdatatable.gitlab.io/data.table/reference/data.table.html#examples) produced by `example(data.table)`

### Cheatsheets

<a href="https://raw.githubusercontent.com/rstudio/cheatsheets/master/datatable.pdf"><img src="https://raw.githubusercontent.com/rstudio/cheatsheets/master/pngs/datatable.png" width="615" height="242"/></a>
