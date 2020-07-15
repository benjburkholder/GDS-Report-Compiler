"""
GRC Package Checker Moduler
"""
import os
import json
import subprocess


def find_package_by_name(name: str, packages: list):
    found = [
        item for item in packages if item['name'] == name
    ]
    if found:
        return found[0]
    else:
        return {}


class PackageChecker:

    win_os_name = 'nt'
    win_check_arg = 'WHERE'

    def check_command_line_tool_installed(self, name: str) -> bool:
        if os.name == self.win_os_name:
            out = subprocess.Popen(
                [
                    self.win_check_arg,
                    name
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT
            )
            stdout, stderr = out.communicate()
            assert stderr is None, "An issue occurred testing for command line application installation"
            if name in str(stdout):
                return True
            else:
                return False
        else:
            raise AssertionError(
                os.name + " operating systems are not supported at this time. Please consult documentation"
            )

    required_command_line_tools = [
        'lmpy',  # linkmedia package manager
    ]

    def check_requirements_installed(self):
        for tool in self.required_command_line_tools:
            result = self.check_command_line_tool_installed(
                name=tool
            )
            if result is False:
                raise AssertionError(
                    "Required tool " + tool + " is not installed. Please install and try again."
                )
        return


    default_installed_packages = []

    def _get_installed_package_path(self):
        return os.path.join(
            os.path.abspath(os.path.dirname(__file__)),
            'tmp',
            'installed.json'
        )

    def get_installed_packages(self):
        installed_package_path = self._get_installed_package_path()
        try:
            with open(installed_package_path, 'r') as file:
                packages = json.load(file)
        except FileNotFoundError:
            with open(installed_package_path, 'w') as file:
                file.write(json.dumps(self.default_installed_packages))
                packages = self.default_installed_packages
        return packages

    def write_installed_packages(self, package_list: list):
        installed_package_path = self._get_installed_package_path()
        with open(installed_package_path, 'w') as file:
            file.write(json.dumps(package_list))
        return True
