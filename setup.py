#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    Use setup.cfg to configure your project.

    This file was generated with PyScaffold 3.1.
    PyScaffold helps you to put up the scaffold of your new Python project.
    Learn more under: https://pyscaffold.org/
"""
import sys

from pkg_resources import require, VersionConflict
from setuptools import setup


try:
    require("setuptools>=38.3")
except VersionConflict:
    import pyscaffold.contrib.setuptools_scm.integration

    def restore_setup_cfg_version(dist, keyboard, value):
        import configparser

        c = configparser.ConfigParser()
        c.read("setup.cfg")
        dist.metadata.version = c["metadata"]["version"]

    pyscaffold.contrib.setuptools_scm.integration.version_keyword = (
        restore_setup_cfg_version
    )


    print("Error: version of setuptools is too old (<38.3)!")
    sys.exit(1)


if __name__ == "__main__":
    setup(use_pyscaffold=True)
