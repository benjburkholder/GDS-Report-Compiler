"""
Update Manager Module
"""
import os
import json
import pathlib
import requests
import zipfile
import shutil


class UpdateManager:
    """
    Class responsible for handling remote updates of static platform components
    ========================================================================================================
    """

    base_uri = 'https://api.github.com/repos/:username/:repository/releases'

    update_key = 'zipball_url'

    def __init__(self, auth_token: str, username: str, repository: str):
        self.headers = {
            'Authorization': 'token ' + auth_token
        }
        self.username = username
        self.repository = repository
        self.release_version = self._get_latest_release_version()

    current_release_idx = 0  # first item back is the latest release from github

    current_release_version_key = 'tag_name'

    current_release_archive_key = 'zipball_url'

    def _get_latest_release_version(self) -> str:
        """
        Finds and retrieves the latest version number based on release recency
        ====================================================================================================
        :return:
        """
        releases = self._get_releases()
        current_release = releases[self.current_release_idx]
        return current_release[self.current_release_version_key]

    def download_latest(self):
        """
        Finds and downloads the latest release .zip archive to the project /tmp directory
        ====================================================================================================
        :return:
        """
        if os.path.exists(self.__get_save_path()):
            self.clear_update_zone()
        releases = self._get_releases()
        current_release = releases[self.current_release_idx]
        self.release_version = current_release[self.current_release_version_key]
        archive_url = current_release.get(self.current_release_archive_key)
        assert archive_url, "No url provided for release archive " + str(current_release)
        archive = self._get_archive(
            archive_url=archive_url
        )
        # unzip the archive into the base dir of the project into /tmp
        self._write_archive(archive=archive)
        self._unzip_archive()

    bad_response_text = "Bad response from GitHub servers, "

    def _get_archive(self, archive_url: str):
        """
        Make a secure HTTP request to get the .zip release archive by URL
        ====================================================================================================
        :param archive_url:
        :return:
        """
        response = requests.get(
            url=archive_url,
            headers=self.headers
        )
        assert response.status_code == 200, self.bad_response_text + response.text
        return response

    def _get_releases(self):
        """
        Make a secure HTTP request to get the platform's releases
        ====================================================================================================
        :return:
        """
        # make the request and parse the response for the update_key
        url = self.base_uri.replace(':username', self.username).replace(':repository', self.repository)
        response = requests.get(
            url=url,
            headers=self.headers
        )
        assert response.status_code == 200, self.bad_response_text + response.text
        return response.json()

    project_root_idx = 1  # two directories down from here

    def __get_project_root(self):
        """
        Returns the global location of the project root
        ====================================================================================================
        :return:
        """
        return pathlib.Path(__file__).parents[self.project_root_idx]

    tmp_dir_name = 'tmp'

    def __get_save_path(self):
        """
        Returns the global location of the projects /tmp directory where updates are made and handled
        ====================================================================================================
        :return:
        """
        return os.path.join(
            self.__get_project_root(),
            self.tmp_dir_name
        )

    archive_file_name = 'archive.zip'

    def __get_write_path(self):
        """
        Returns the path where the archive contents should be written to
        ====================================================================================================
        :return:
        """
        return os.path.join(
            self.__get_save_path(),
            self.archive_file_name
        )

    unzip_dir_name = 'bin'

    def __get_unzip_path(self):
        """
        Returns the path where the release archive contents should be unzipped to
        ====================================================================================================
        :return:
        """
        return os.path.join(self.__get_save_path(), self.unzip_dir_name)

    grc_parent_dir_name = 'GRC'

    def __get_parent_download_path(self):
        """
        Returns the path where the release unzipped contents live
        ====================================================================================================
        :return:
        """
        return os.path.join(self.__get_unzip_path(), self.grc_parent_dir_name)

    archive_chunk_size = 128

    def _write_archive(self, archive):
        """
        Writes the binary .zip data to the archive directory for later processing
        ====================================================================================================
        :param archive:
        :return:
        """
        save_path = self.__get_save_path()
        write_path = self.__get_write_path()
        if not os.path.exists(save_path):
            os.mkdir(save_path)
        with open(write_path, 'wb') as fd:
            for chunk in archive.iter_content(chunk_size=self.archive_chunk_size):
                fd.write(chunk)

    def _unzip_archive(self):
        """
        Unzips the release archive into the working /tmp directory
        ====================================================================================================
        :return:
        """
        write_path = self.__get_write_path()
        unzip_path = self.__get_unzip_path()
        if not os.path.exists(unzip_path):
            os.mkdir(unzip_path)
        with zipfile.ZipFile(write_path, 'r') as zip_ref:
            zip_ref.extractall(unzip_path)
        old_parent_dir_name = os.path.join(unzip_path, os.listdir(unzip_path)[0])
        new_parent_dir_name = self.__get_parent_download_path()
        os.rename(old_parent_dir_name, new_parent_dir_name)

    excluded_update_files = [
        '.gitattributes',
        '.idea',
        '.github'
    ]

    excluded_update_directories = [
        'conf',
        'user',
        'venv',
        'tmp'
    ]

    def perform_update(self, dir_name: str = None):
        """
        Walk through the project and update files that are not protected as:
            - /user: specific to the implementation or client
            - /conf: sensitive configuration data, also specific to implementation
            - system: (venv, tmp, etc.) any directories that are used by the Python / GRC ecosystem
                and should not be changed
        ====================================================================================================
        :param dir_name:
        :return:
        """
        parent_dir_name = self.__get_parent_download_path()
        dst_dir_name = self.__get_project_root()
        if dir_name:
            parent_dir_name = os.path.join(parent_dir_name, dir_name)
            dst_dir_name = os.path.join(dst_dir_name, dir_name)
        # walk through downloaded directory
        files = os.listdir(parent_dir_name)
        # for each update-capable file, replace (if exists) or add its equivalent in the live project
        for file in files:
            if file not in self.excluded_update_files and '.' in file:
                src_file = os.path.join(parent_dir_name, file)
                dst_file = os.path.join(dst_dir_name, file)
                # check for changes
                if self._compare_file_contents(src_file=src_file, dst_file=dst_file):
                    if os.path.isfile(dst_file):
                        os.unlink(dst_file)
                    shutil.copyfile(src_file, dst_file)
            else:
                if '.' not in file:
                    if file not in self.excluded_update_directories:
                        if dir_name:
                            _dir_name = os.path.join(dir_name, file)
                        else:
                            _dir_name = file
                        self.perform_update(dir_name=_dir_name)
        # update the 'version' attribute of app.json to the version stored in this class
        self.update_app_version()
        self.clear_update_zone()
        return

    @staticmethod
    def _compare_file_contents(src_file: str, dst_file: str) -> bool:
        """
        Static helper method to determine if two files are equal by their contents or not
        ====================================================================================================
        :param src_file:
        :param dst_file:
        :return:
        """
        with open(src_file, 'r') as src:
            scr_file_contents = src.read()
        with open(dst_file, 'r') as dst:
            dst_file_contents = dst.read()
        return scr_file_contents != dst_file_contents

    def update_app_version(self):
        """
        Loads the app.json configuration and updates its 'version' attribute in line with the latest release
        ====================================================================================================
        :return:
        """
        app_config_path = self.__get_app_config_path()
        with open(app_config_path, 'r') as cnf:
            app_config = json.load(cnf)
        app_config['version'] = self.release_version
        with open(app_config_path, 'w') as cnf:
            cnf.write(json.dumps(app_config))

    def __get_app_config_path(self):
        """
        Returns the path where the app.json configuration file lives
        ====================================================================================================
        :return:
        """
        return os.path.join(
            self.__get_project_root(),
            'conf',
            'stored',
            'app.json'
        )

    def clear_update_zone(self):
        """
        Recursively deletes files from the /tmp directory where the archive is unzipped to
        ====================================================================================================
        :return:
        """
        shutil.rmtree(self.__get_save_path())
