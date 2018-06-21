# pystgres
:elephant: In-memory python implementation of a postgres server for use as a mock

This is currently in the personal-project state. The goals of this project are:
- Have a drop-in replacement for common postgres operations, potentially for use as a mock to python tests
- Have fun hacking on something
  - and learning more about postgres by poking at its edge cases

The goals of this project are not:
- Performance
- Most of [ACID](https://en.wikipedia.org/wiki/ACID)
- Porting the actual postgres source code to python

Relies on [my fork of psqlparse](https://github.com/orez-/psqlparse)
