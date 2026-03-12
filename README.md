[![Code of Conduct](https://img.shields.io/badge/%E2%9D%A4-code%20of%20conduct-blue.svg?style=flat)](https://github.com/edgi-govdata-archiving/overview/blob/main/CONDUCT.md) &nbsp;[![Project Status Board](https://img.shields.io/badge/✔-Project%20Status%20Board-green.svg?style=flat)](https://github.com/orgs/edgi-govdata-archiving/projects/32)


# web-monitoring-processing

A component of the EDGI [Web Monitoring Project](https://github.com/edgi-govdata-archiving/web-monitoring).


## Overview of this component's tasks

This component is intended to hold various backend tools serving different tasks:

1. Query external sources of captured web pages (e.g. Internet Archive, Page
   Freezer, Sentry), and formulate a request for importing their version and
   page metadata into web-monitoring-db.
2. Query web-monitoring-db for new Changes, analyze them in an automated
   pipeline to assign priority and/or filter out uninteresting ones, and submit
   this information back to web-monitoring-db.


## Development status

Working and Under Active Development:

* A Python API to the web-monitoring-db Rails app in ``web_monitoring.db``
* Python functions and a command-line tool for importing snapshots from the
  Internet Archive into web-monitoring-db.

Legacy projects that may be revisited:
* [Example HTML](https://github.com/edgi-govdata-archiving/web-monitoring-processing/tree/main/archives) providing useful test cases.


## Installation Instructions

1. Get Python 3.10 or later. If you don't have the right version, we recommend using
   [conda](https://conda.io/miniconda.html) or [pyenv](https://github.com/pyenv/pyenv) to install it. (You don't need admin privileges to install or use them, and they won't interfere with any other installations of Python already on your system.)

2. Install libxml2 and libxslt. (This package uses lxml, which requires your system to have the libxml2 and libxslt libraries.)

    On MacOS, use Homebrew:

    ```sh
    brew install libxml2
    brew install libxslt
    ```

    On Debian Linux:

    ```sh
    apt-get install libxml2-dev libxslt-dev
    ```

    On other systems, the packages might have slightly different names.

3. Install the package.

    ```sh
    pip install .
    ```

4. Copy the script `.env.example` to `.env` and supply any local configuration
   info you need. (Only some of the package's functionality requires this.)
   Apply the configuration:

    ```sh
    source .env
    ```

5. See module comments and docstrings for more usage information. Also see the
   command line tool ``wm``, which is installed with the package. For help, use

   ```sh
   wm --help
   ```

6. To run the tests or build the documentation, first install the development
   dependencies.

   ```sh
   pip install '.[dev]'
   ```

7. To build the docs:

   ```sh
   cd docs
   make html
   ```

8. To run the tests:

   ```sh
   python run_tests.py
   ```

   Any additional arguments are passed through to `py.test`.


## Releases

We try to make sure the code in this repo’s `main` branch is always in a stable, usable state, but occasionally coordinated functionality may be written across multiple commits. If you are depending on this package from another Python program, you may wish to install from the `release` branch instead:

```sh
$ pip install git+https://github.com/edgi-govdata-archiving/web-monitoring-processing@release
```

You can also list the `git+https:` URL above in a pip *requirements* file.

We usually create *merge commits* on the `release` branch that note the PRs included in the release or any other relevant notes (e.g. [`Release #302 and #313.`](https://github.com/edgi-govdata-archiving/web-monitoring-processing/commit/446ae83e121ec8c2207b2bca563364cafbdf8ce0)).


## Code of Conduct

This repository falls under EDGI's [Code of Conduct](https://github.com/edgi-govdata-archiving/overview/blob/main/CONDUCT.md).


## Contributors

This project wouldn’t exist without a lot of amazing people’s help. Thanks to the following for all their contributions! See our [contributing guidelines](https://github.com/edgi-govdata-archiving/web-monitoring-processing/blob/main/CONTRIBUTING.md) to find out how you can help.

<!-- ALL-CONTRIBUTORS-LIST:START -->
| Contributions | Name |
| ----: | :---- |
| [💻](# "Code") [⚠️](# "Tests") [🚇](# "Infrastructure") [📖](# "Documentation") [💬](# "Answering Questions") [👀](# "Reviewer") | [Dan Allan](https://github.com/danielballan) |
| [💻](# "Code") | [Vangelis Banos](https://github.com/vbanos) |
| [💻](# "Code") [📖](# "Documentation") | [Chaitanya Prakash Bapat](https://github.com/ChaiBapchya) |
| [💻](# "Code") | [Kevin (Hany) Bastawrous](https://github.com/kevo-1) |
| [💻](# "Code") [⚠️](# "Tests") [🚇](# "Infrastructure") [📖](# "Documentation") [💬](# "Answering Questions") [👀](# "Reviewer") | [Rob Brackett](https://github.com/Mr0grog) |
| [💻](# "Code") | [Stephen Buckley](https://github.com/StephenAlanBuckley) |
| [💻](# "Code") [📖](# "Documentation") [📋](# "Organizer") | [Ray Cha](https://github.com/weatherpattern) |
| [💻](# "Code") [⚠️](# "Tests") | [Janak Raj Chadha](https://github.com/janakrajchadha) |
| [💻](# "Code") | [Autumn Coleman](https://github.com/AutumnColeman) |
| [💻](# "Code") | [Luming Hao](https://github.com/lh00000000) |
| [🤔](# "Ideas and Planning") | [Mike Hucka](https://github.com/mhucka) |
| [💻](# "Code") | [Stuart Lynn](https://github.com/stuartlynn) |
| [💻](# "Code") [⚠️](# "Tests") | [Julian Mclain](https://github.com/julianmclain) |
| [💻](# "Code") | [Allan Pichardo](https://github.com/allanpichardo) |
| [📖](# "Documentation") [📋](# "Organizer") | [Matt Price](https://github.com/titaniumbones) |
| [💻](# "Code") | [Mike Rotondo](https://github.com/mrotondo) |
| [📖](# "Documentation") | [Susan Tan](https://github.com/ArcTanSusan) |
| [💻](# "Code") [⚠️](# "Tests") | [Fotis Tsalampounis](https://github.com/ftsalamp) |
| [📖](# "Documentation") [📋](# "Organizer") | [Dawn Walker](https://github.com/dcwalk) |
<!-- ALL-CONTRIBUTORS-LIST:END -->

(For a key to the contribution emoji or more info on this format, check out [“All Contributors.”](https://github.com/kentcdodds/all-contributors))


## License & Copyright

Copyright (C) 2017-2021 Environmental Data and Governance Initiative (EDGI)

This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, version 3.0.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.

See the [`LICENSE`](https://github.com/edgi-govdata-archiving/webpage-versions-processing/blob/main/LICENSE) file for details.
