# Copyright 2021 Splunk Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import unittest
from importlib.metadata import distributions
from pathlib import Path


class TestPackageMetadata(unittest.TestCase):
    def test_requires_python_311_or_newer(self):
        installed_distribution = next(
            distribution
            for distribution in distributions(name="python-splunk-logging")
            if Path(distribution.locate_file("")).is_relative_to(sys.prefix)
        )

        self.assertEqual(installed_distribution.metadata["Requires-Python"], ">=3.11")
