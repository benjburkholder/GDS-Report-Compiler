"""
GRC Package Installer Module
"""
# STANDARD IMPORTS
import os
import subprocess

# PLATFORM IMPORTS
from ..stdlib import get_venv_path, get_base_path


class PackageInstaller:

    path = os.path.abspath(os.path.dirname(__file__))
    base_path = get_base_path()
    venv_path = get_venv_path()

    def _get_venv_activate_path(self):
        if os.name == 'nt':
            return os.path.join(
                self.base_path,
                'venv',
                'Scripts',
                'activate'
            )
        else:
            return os.path.join(
                self.base_path,
                'venv',
                'bin',
                'activate'
            )

    def _create_batch_file_path(self, file_name: str):
        path = os.path.join(
            self.path,
            'tmp',
            file_name
        )
        return path

    install_file_name_win = 'install.bat'

    install_utility = 'lmpy'
    install_utility_quit = 'q'

    def _install_package(self, package: dict) -> bool:
        if os.name == 'nt':
            bat_path = self._create_batch_file_path(
                file_name=self.install_file_name_win
            )
            with open(bat_path, 'w') as file:
                file.write(
                    (
                        f'{self._get_venv_activate_path()} '
                        f'& {self.install_utility} '
                        f'--cli '
                        f'--name={package["name"]} '
                        f'--ver={package["version"]}\n'
                    )
                )
            process = subprocess.Popen(
                [
                    bat_path
                ],
                stdout=subprocess.PIPE,
                stdin=subprocess.PIPE,
                stderr=subprocess.STDOUT
            )
            std_out = process.communicate()[0]
            print(std_out.decode())
            # open a shell with subprocess and assert there is no standard error
            return True
        else:
            raise AssertionError('Operating system ' + os.name + ' not supported')

    def install_package(self, package: dict) -> bool:
        result = self._install_package(package=package)
        assert result, "Package install failed"
        return result

    def update_package(self, package: dict) -> bool:
        self.remove_package(package=package)
        self.install_package(package=package)
        return True

    def _remove_package(self, package: dict) -> bool:
        if os.name == 'nt':
            bat_path = self._create_batch_file_path(
                file_name=self.install_file_name_win
            )
            with open(bat_path, 'w') as file:
                file.write(
                    (
                        f'{self._get_venv_activate_path()} '
                        f'& pip uninstall {package["pip_alias"]} --yes'
                    )
                )
            process = subprocess.Popen(
                [
                    bat_path
                ],
                stdout=subprocess.PIPE,
                stdin=subprocess.PIPE,
                stderr=subprocess.STDOUT
            )
            std_out, std_err = process.communicate()
            print(std_out.decode())
            return True
        else:
            raise AssertionError('Operating system ' + os.name + ' not supported')

    def remove_package(self, package: dict) -> bool:
        result = self._remove_package(package=package)
        assert result, "Package removal failed"
        return result

    def freeze_requirements(self, package_list: list) -> None:
        # cd to base path
        # execute pip freeze > requirements.txt
        # open requirements.txt in read mode
        # readlines()
        # for each package in package_list, check if its pip_alias is present
        # if so, delete the entry (mark for deletion)
        # re-write requirements.txt without proprietary packages
        return
