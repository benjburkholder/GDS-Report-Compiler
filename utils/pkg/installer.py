"""
GRC Package Installer Module
"""
# STANDARD IMPORTS
import os

# PLATFORM IMPORTS
from ..stdlib import get_venv_path, get_base_path


class PackageInstaller:

    base_path = get_base_path()
    venv_path = get_venv_path()

    def install_package(self, package: dict) -> None:
        # based on os...
        # write a batch file in /tmp to...
        # activate the venv
        # install the package with version specified (updates to lmpy required)
        # open a shell with subprocess and assert there is no standard error
        return

    def update_package(self, package: dict) -> None:
        # based on os...
        # write a batch file in /tmp to...
        # activate the venv
        # uninstall the package
        # install the package with version specified
        # open a shell with subprocess and assert there is no standard error
        return

    def remove_package(self, package: dict) -> None:
        # based on os...
        # write a batch file in /tmp to...
        # activate the venv
        # uninstall the package
        # install the package with version specified
        # open a shell with subprocess and assert there is no standard error
        return

    def freeze_requirements(self, package_list: list) -> None:
        # execute pip freeze > requirements.txt
        # open requirements.txt in read mode
        # readlines()
        # for each package in package_list, check if its pip_alias is present
        # if so, delete the entry (mark for deletion)
        # re-write requirements.txt without proprietary packages
        return
