.. highlight:: shell

============
Contributing
============

Contributions are welcome, and they are greatly appreciated! Every little bit
helps, and credit will always be given.

You can contribute in many ways:

Types of Contributions
----------------------

Report Bugs
~~~~~~~~~~~

Report bugs at https://github.com/projectnessie/pynessie/issues.

If you are reporting a bug, please include:

* Your operating system name and version.
* Any details about your local setup that might be helpful in troubleshooting.
* Detailed steps to reproduce the bug.

Fix Bugs
~~~~~~~~

Look through the GitHub issues for bugs. Anything tagged with "bug" and "help
wanted" is open to whoever wants to implement it.

Implement Features
~~~~~~~~~~~~~~~~~~

Look through the GitHub issues for features. Anything tagged with "enhancement"
and "help wanted" is open to whoever wants to implement it.

Write Documentation
~~~~~~~~~~~~~~~~~~~

Python API and CLI for Nessie could always use more documentation, whether as part of the
official Python API and CLI for Nessie docs, in docstrings, or even on the web in blog posts,
articles, and such.

Submit Feedback
~~~~~~~~~~~~~~~

The best way to send feedback is to file an issue at https://github.com/projectnessie/pynessie/issues.

If you are proposing a feature:

* Explain in detail how it would work.
* Keep the scope as narrow as possible, to make it easier to implement.
* Remember that this is a volunteer-driven project, and that contributions
  are welcome :)

Get Started!
------------

Ready to contribute? Here's how to set up `pynessie` for local development.

1. Fork the `pynessie` repo on GitHub.
2. Clone your fork locally::

    git clone https://github.com/projectnessie/pynessie.git

3. Install your local copy into a virtualenv. Assuming you have virtualenvwrapper installed, this is how you set up your fork for local development::

    # Change directory to the root directory of your
    # https://github.com/projectnessie/pynessie.git clone.
    cd pynessie/
    # setup the virtual environment (only needed once)
    [ ! -d venv/ ] && virtualenv -p $(which python3) venv
    # Run setup.py for pynessie
    python setup.py develop

4. Create a branch for local development::

    git checkout -b name-of-your-bugfix-or-feature

   Now you can make your changes locally.

5. When you're done making changes, check that your changes pass flake8 and the
   tests, including testing other Python versions with tox and all docs have been generated::

    tox

   To get flake8 and tox, just pip install them into your virtualenv.

6. Commit your changes and push your branch to GitHub::

    git add .
    git commit -m "Your detailed description of your changes."
    git push origin name-of-your-bugfix-or-feature

7. Submit a pull request through the GitHub website.

Code style
----------

Python code style issues can be fixed from the command line using::

    # Change directory to the root directory of your
    # https://github.com/projectnessie/pynessie.git clone.
    cd pynessie/
    # setup the virtual environment (only needed once)
    [ ! -d venv/ ] && virtualenv -p $(which python3) venv
    # activate the virtual environment
    . venv/bin/activate
    # Install or update the dependencies as usual
    pip install -U -r requirements_lint.txt
    # Run 'black' in the pynessie/ and tests/ subdirectoris
    black pynessie tests

Pull Request Guidelines
-----------------------

Before you submit a pull request, check that it meets these guidelines:

1. The pull request should include tests.
2. If the pull request adds functionality, the docs should be updated. Put
   your new functionality into a function with a docstring, and add the
   feature to the list in README.rst.
3. The pull request should work for Python 3.8 up to 3.11. Check
   the pull request status and make sure that the tests pass for all
   supported Python versions.

Tips
----

To run a subset of tests::

    $ pytest tests.test_pynessie

To fix code style issues::

    $ black pynessie/ tests/

If you are using podman::

    export DOCKER_HOST=unix:///run/user/$(id -u)/podman/podman.sock
