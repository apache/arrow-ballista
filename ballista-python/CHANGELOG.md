<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Changelog

## [Unreleased](https://github.com/datafusion-contrib/datafusion-python/tree/HEAD)

[Full Changelog](https://github.com/datafusion-contrib/datafusion-python/compare/0.5.1...HEAD)

**Merged pull requests:**

- use \_\_getitem\_\_ for df column selection [\#41](https://github.com/datafusion-contrib/datafusion-python/pull/41) ([Jimexist](https://github.com/Jimexist))
- fix demo in readme [\#40](https://github.com/datafusion-contrib/datafusion-python/pull/40) ([Jimexist](https://github.com/Jimexist))
- Implement select_columns [\#39](https://github.com/datafusion-contrib/datafusion-python/pull/39) ([andygrove](https://github.com/andygrove))
- update readme and changelog [\#38](https://github.com/datafusion-contrib/datafusion-python/pull/38) ([Jimexist](https://github.com/Jimexist))
- Add PyDataFrame.explain [\#36](https://github.com/datafusion-contrib/datafusion-python/pull/36) ([andygrove](https://github.com/andygrove))
- Release 0.5.0 [\#34](https://github.com/datafusion-contrib/datafusion-python/pull/34) ([Jimexist](https://github.com/Jimexist))
- disable nightly in workflow [\#33](https://github.com/datafusion-contrib/datafusion-python/pull/33) ([Jimexist](https://github.com/Jimexist))
- update requirements to 37 and 310, update readme [\#32](https://github.com/datafusion-contrib/datafusion-python/pull/32) ([Jimexist](https://github.com/Jimexist))
- Add custom global allocator [\#30](https://github.com/datafusion-contrib/datafusion-python/pull/30) ([matthewmturner](https://github.com/matthewmturner))
- Remove pandas dependency [\#25](https://github.com/datafusion-contrib/datafusion-python/pull/25) ([matthewmturner](https://github.com/matthewmturner))
- upgrade datafusion and pyo3 [\#20](https://github.com/datafusion-contrib/datafusion-python/pull/20) ([Jimexist](https://github.com/Jimexist))
- update maturin 0.12+ [\#17](https://github.com/datafusion-contrib/datafusion-python/pull/17) ([Jimexist](https://github.com/Jimexist))
- Update README.md [\#16](https://github.com/datafusion-contrib/datafusion-python/pull/16) ([Jimexist](https://github.com/Jimexist))
- apply cargo clippy --fix [\#15](https://github.com/datafusion-contrib/datafusion-python/pull/15) ([Jimexist](https://github.com/Jimexist))
- update test workflow to include rust clippy and check [\#14](https://github.com/datafusion-contrib/datafusion-python/pull/14) ([Jimexist](https://github.com/Jimexist))
- use maturin 0.12.6 [\#13](https://github.com/datafusion-contrib/datafusion-python/pull/13) ([Jimexist](https://github.com/Jimexist))
- apply cargo fmt [\#12](https://github.com/datafusion-contrib/datafusion-python/pull/12) ([Jimexist](https://github.com/Jimexist))
- use stable not nightly [\#11](https://github.com/datafusion-contrib/datafusion-python/pull/11) ([Jimexist](https://github.com/Jimexist))
- ci: test against more compilers, setup clippy and fix clippy lints [\#9](https://github.com/datafusion-contrib/datafusion-python/pull/9) ([cpcloud](https://github.com/cpcloud))
- Fix use of importlib.metadata and unify requirements.txt [\#8](https://github.com/datafusion-contrib/datafusion-python/pull/8) ([cpcloud](https://github.com/cpcloud))
- Ship the Cargo.lock file in the source distribution [\#7](https://github.com/datafusion-contrib/datafusion-python/pull/7) ([cpcloud](https://github.com/cpcloud))
- add \_\_version\_\_ attribute to datafusion object [\#3](https://github.com/datafusion-contrib/datafusion-python/pull/3) ([tfeda](https://github.com/tfeda))
- fix ci by fixing directories [\#2](https://github.com/datafusion-contrib/datafusion-python/pull/2) ([Jimexist](https://github.com/Jimexist))
- setup workflow [\#1](https://github.com/datafusion-contrib/datafusion-python/pull/1) ([Jimexist](https://github.com/Jimexist))

## [0.5.1](https://github.com/datafusion-contrib/datafusion-python/tree/0.5.1) (2022-03-15)

[Full Changelog](https://github.com/datafusion-contrib/datafusion-python/compare/0.5.1-rc1...0.5.1)

## [0.5.1-rc1](https://github.com/datafusion-contrib/datafusion-python/tree/0.5.1-rc1) (2022-03-15)

[Full Changelog](https://github.com/datafusion-contrib/datafusion-python/compare/0.5.0...0.5.1-rc1)

## [0.5.0](https://github.com/datafusion-contrib/datafusion-python/tree/0.5.0) (2022-03-10)

[Full Changelog](https://github.com/datafusion-contrib/datafusion-python/compare/0.5.0-rc2...0.5.0)

## [0.5.0-rc2](https://github.com/datafusion-contrib/datafusion-python/tree/0.5.0-rc2) (2022-03-10)

[Full Changelog](https://github.com/datafusion-contrib/datafusion-python/compare/0.5.0-rc1...0.5.0-rc2)

**Closed issues:**

- Add support for Ballista [\#37](https://github.com/datafusion-contrib/datafusion-python/issues/37)
- Implement DataFrame.explain [\#35](https://github.com/datafusion-contrib/datafusion-python/issues/35)

## [0.5.0-rc1](https://github.com/datafusion-contrib/datafusion-python/tree/0.5.0-rc1) (2022-03-09)

[Full Changelog](https://github.com/datafusion-contrib/datafusion-python/compare/4c98b8e9c3c3f8e2e6a8f2d1ffcfefda344c4680...0.5.0-rc1)

**Closed issues:**

- Investigate exposing additional optimizations [\#28](https://github.com/datafusion-contrib/datafusion-python/issues/28)
- Use custom allocator in Python build [\#27](https://github.com/datafusion-contrib/datafusion-python/issues/27)
- Why is pandas a requirement? [\#24](https://github.com/datafusion-contrib/datafusion-python/issues/24)
- Unable to build [\#18](https://github.com/datafusion-contrib/datafusion-python/issues/18)
- Setup CI against multiple Python version [\#6](https://github.com/datafusion-contrib/datafusion-python/issues/6)

\* _This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)_
