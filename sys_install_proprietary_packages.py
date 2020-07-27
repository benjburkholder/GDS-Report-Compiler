"""
System Script :: Install Proprietary Packages

This script helps ensure you have the proper proprietary packages installed (from GitHub)
given the REQUIRED_PACKAGES global in conf/static.py
"""
# STANDARD IMPORTS
import os
import traceback

# PLATFORM IMPORTS
from utils import grc
from utils.cls.core import Customizer
from conf.static import REQUIRED_PACKAGES
from utils.cls.pltfm.gmail import send_error_email
from utils.pkg.installer import PackageInstaller
from utils.pkg.checker import find_package_by_name, PackageChecker
SCRIPT_NAME = os.path.basename(__file__)
DEBUG = False
if DEBUG:
    print('WARN: Error reporting disabled. DEBUG = TRUE')


def main() -> None:

    # instantiate required class modules
    pc = PackageChecker()
    pi = PackageInstaller()

    # ensure all pre-requisites are installed before proceeding
    pc.check_requirements_installed()  # will throw an error on exception

    # read the packages from REQUIRED_PACKAGES
    conf_packages = REQUIRED_PACKAGES

    # read the packages from tmp/packages.json (or create packages.json if it does not exist)
    packages = pc.get_installed_packages()

    for package in conf_packages:
        installed = find_package_by_name(
            name=package['name'],
            packages=packages
        )
        if not installed:
            pi.install_package(
                package=package
            )
        elif package['version'] != installed['version']:
            pi.update_package(
                package=package
            )

    for package in packages:
        configured = find_package_by_name(
            name=package['name'],
            packages=conf_packages
        )
        if not configured:
            pi.remove_package(
                package=package
            )

    # pip freeze
    # open requirements.txt and wipe out any entries matching each packages 'pip_alias'
    pi.freeze_requirements(
        package_list=conf_packages
    )

    # update installed.json
    pc.write_installed_packages(package_list=conf_packages)

    return


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        if not DEBUG:
            send_error_email(
                client_name=Customizer().client,
                script_name=SCRIPT_NAME,
                to=Customizer().recipients,
                error=error,
                stack_trace=traceback.format_exc(),
                engine=grc.create_application_sql_engine()
            )
        raise
