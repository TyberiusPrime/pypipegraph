"""
The MIT License (MIT)

Copyright (c) 2012, Florian Finkernagel <finkernagel@imt.uni-marburg.de>

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import pytest
import pypipegraph as ppg
from .shared import assertRaises


@pytest.mark.usefixtures("new_pipegraph")
class TestUtils:
    def test_assert_uniqueness_simple(self):
        class Dummy:
            def __init__(self, name):
                self.name = name
                ppg.util.assert_uniqueness_of_object(self)

        Dummy("shu")

        def inner():
            Dummy("shu")

        assertRaises(ValueError, inner)

    def test_assert_uniqueness_ok(self):
        class Dummy:
            def __init__(self, name):
                self.name = name
                ppg.util.assert_uniqueness_of_object(self)

        Dummy("shu")
        Dummy("sha")

        def inner():
            Dummy("shu")

        assertRaises(ValueError, inner)

    def test_assert_uniqueness_ok_multi_classes(self):
        class Dummy:
            def __init__(self, name):
                self.name = name
                ppg.util.assert_uniqueness_of_object(self)

        class Dummy2:
            def __init__(self, name):
                self.name = name
                ppg.util.assert_uniqueness_of_object(self)

        Dummy("shu")
        Dummy2("shu")

        def inner():
            Dummy("shu")

        assertRaises(ValueError, inner)

    def test_assert_uniqueness_raises_slashes(self):
        class Dummy:
            def __init__(self, name):
                self.name = name
                ppg.util.assert_uniqueness_of_object(self)

        Dummy("shu")

        def inner():
            Dummy("shu/sha")

        assertRaises(ValueError, inner)

    def test_assert_uniqueness_raises_also_check(self):
        class Dummy:
            def __init__(self, name):
                self.name = name
                ppg.util.assert_uniqueness_of_object(self)

        class Dummy2:
            def __init__(self, name):
                self.name = name
                ppg.util.assert_uniqueness_of_object(self, also_check=Dummy)

        Dummy("shu")

        def inner():
            Dummy2("shu")

        assertRaises(ValueError, inner)

    def test_assert_uniqueness_raises_also_check_no_instance_of_second_class(self):
        class Dummy:
            def __init__(self, name):
                self.name = name
                ppg.util.assert_uniqueness_of_object(self)

        class Dummy2:
            def __init__(self, name):
                self.name = name
                ppg.util.assert_uniqueness_of_object(self, also_check=Dummy)

        # a = Dummy('shu')
        # does not raise of course...
        Dummy2("shu")

        def inner():
            Dummy2("shu")

        assertRaises(ValueError, inner)

    def test_assert_uniqueness_raises_also_check_list(self):
        class Dummy:
            def __init__(self, name):
                self.name = name
                ppg.util.assert_uniqueness_of_object(self)

        class Dummy2:
            def __init__(self, name):
                self.name = name
                ppg.util.assert_uniqueness_of_object(self, also_check=[Dummy])

        Dummy("shu")

        def inner():
            Dummy2("shu")

        assertRaises(ValueError, inner)
