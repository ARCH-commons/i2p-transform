## Design: ETL Tasks and SQL Scripts

The main tasks are in:

  - *epic_flowsheets* -- ETL tasks for Epic Flowsheets to i2b2

which is based on a design in:

  - *etl_tasks* -- Source-agnostic Luigi ETL Task support
  - *script_lib* -- library of SQL scripts
  - *sql_syntax* -- break SQL scripts into statements, etc.

Tasks such as `epic_flowsheets.FlowsheetViews` are based on SQL
scripts such as `sql_scripts/epic_flowsheets_transform.sql` wrapped in
a `etl_tasks.SqlScriptTask`.


### SQL Script Library Design, Style and Conventions

Each script should start with a header comment and some
dependency-checking queries. See `script_lib.py` for details.

SQL should be written in lowercase, indented 2 spaces, 120 maximum line
length. More details are in the evolving `sql-style.xml` sqldeveloper
style profile.

  - *ISSUE*: sqldeveloper 3 vs. 4 style files?

See also notes on value enumerations in the header of `sql_scripts/cms_keys.pls`.
*TODO*: port those notes from GROUSE.


## Python doctest for story telling and unit testing

Each python module header should tell a story using [doctest][],
i.e. examples that are also unit tests.

You can run them one module at a time:

    (luigi)$ python -m doctest script_lib.py -v
    Trying:
        Script.migrate_fact_upload.title
    Expecting:
        'append data from a workspace table.'
	...
	22 tests in 13 items.
	22 passed and 0 failed.
	Test passed.

Or install [nose][] and run all modules at once:

  (grouse-etl)% nosetests --with-doctest
  ......................
  ----------------------------------------------------------------------
  Ran 22 tests in 0.658s
  
  OK

[doctest]: http://docs.python.org/2/library/doctest.html
[nose]: https://pypi.python.org/pypi/nose/


## Python code style

We appreciate object capability discipline and the "don't call us,
we'll call you" style that facilitates unit testing with mocks.

  - *ISSUE*: Luigi's design doesn't seem to support this idiom.
             Constructors are implict and tasks parameters have to be
             serializable, which works against the usual closure
             object pattern.  Also, the task cache is global mutable
             state.

We avoid mutable state, preferring functional style.

  - *NOTE*: PEP8 tools warn against assinging a lambda to a name,
            suggesting `def` instead. We're fine with it; hence
            `ignore = E731` in `setup.cfg`.


We follow PEP8. The first line of a module or function docstring
should be a short description; if it is blank, either the item is in
an early stage of development or the name and doctests are supposed to
make the purpose obvious.

  - *NOTE*: with static type annotations, the 79 character line
            length limit is awkward; hence we use 99 in `setup.cfg`.

  - *ISSUE*: Dan didn't realize until recently that PEP8 recommends
             triple double quotes over triple single quotes for
             docstrings. He's in the habit of using single quotes
			 to minimize use of the shift key.


## Checking the code

Once dependencies in `requirements.txt` are satisfied, code should
pass tests, style checks, and static type checking:

    $ nosetests && flake8 . && mypy .

_tested with mypy-0.720_

### Checking in emacs

To check with `M-x compile` in emacs, first use `M-x pyvenv-activate`
from the [pyvenv][] package.

To check continuously as you edit, use [flycheck][] and activate
likewise.

[pyvenv]: https://melpa.org/#/pyvenv
[pyvent]: https://melpa.org/#/flycheck


## Luigi Troubleshooting

**ISSUE**: why won't luigi find modules in the current directory?
           Use `PYTHONPATH=. luigi ...` if necessary.

Most diagnostics are self-explanatory; `etl_tasks` includes
`SQLScriptError` and `ConnectionProblem` exception classes intended to
improve diagnostics

One challenging diagnostic is:

    RuntimeError: Unfulfilled dependency at run time: DiagnosesLoad_oracle___dconnol_CMS_DEID_SAMPLE_1438246788671_bd6231c982

It seems to indicate that the `.complete()` test on a required task
fails even after that task has been `.run()`. For example, the `select
count(*)` completion test in a load script might have failed because
of incorrect join constraints.
