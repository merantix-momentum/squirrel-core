"""This module defines specific fixtures for unit tests. Shared fixtures are defined in shared_fixtures.py.

####################################
Please do not import from this file.
####################################

Not importing from conftest is a best practice described in the note here:
https://pytest.org/en/6.2.x/writing_plugins.html#conftest-py-local-per-directory-plugins
"""

from squirrel.integration_test.shared_fixtures import *  # noqa: F401, F403
