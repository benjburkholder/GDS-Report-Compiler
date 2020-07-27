"""
Update Manager Module
"""
import os
import pathlib
import requests
import zipfile
import shutil


class UpdateManager:

    base_uri = 'https://api.github.com/repos/:username/:repository/releases'

    update_key = 'zipball_url'

    def __init__(self, auth_token: str, username: str, repository: str):
        self.params = {
            'access_token': auth_token
        }
        self.username = username
        self.repository = repository

    current_release_idx = 0  # first item back is the latest release from github

    def download_latest(self):
        releases = self._get_releases()
        current_release = releases[self.current_release_idx]
        archive_url = current_release.get('zipball_url')
        assert archive_url, "No url provided for release archive " + str(current_release)
        archive = self._get_archive(
            archive_url=archive_url
        )
        # unzip the archive into the base dir of the project into /tmp
        self._write_archive(archive=archive)
        self._unzip_archive()

    bad_response_text = "Bad response from GitHub servers, "

    def _get_archive(self, archive_url: str):
        response = requests.get(
            url=archive_url,
            params=self.params
        )
        assert response.status_code == 200, self.bad_response_text + response.text
        return response

    def _get_releases(self):
        # make the request and parse the response for the update_key
        url = self.base_uri.replace(':username', self.username).replace(':repository', self.repository)
        response = requests.get(
            url=url,
            params=self.params
        )
        assert response.status_code == 200, self.bad_response_text + response.text
        return response.json()

    tmp_dir_idx = 1  # two directories down from here
    tmp_dir_name = 'tmp'

    def __get_save_path(self):
        return os.path.join(
            pathlib.Path(__file__).parents[self.tmp_dir_idx],
            self.tmp_dir_name
        )

    archive_file_name = 'archive.zip'

    def __get_write_path(self):
        return os.path.join(
            self.__get_save_path(),
            self.archive_file_name
        )

    unzip_dir_name = 'bin'

    def __get_unzip_path(self):
        return os.path.join(self.__get_save_path(), self.unzip_dir_name)

    grc_parent_dir_name = 'GRC'

    def __get_parent_download_path(self):
        return os.path.join(self.__get_unzip_path(), self.grc_parent_dir_name)

    archive_chunk_size = 128

    def _write_archive(self, archive):
        save_path = self.__get_save_path()
        write_path = self.__get_write_path()
        if not os.path.exists(save_path):
            os.mkdir(save_path)
        with open(write_path, 'wb') as fd:
            for chunk in archive.iter_content(chunk_size=self.archive_chunk_size):
                fd.write(chunk)

    def _unzip_archive(self):
        write_path = self.__get_write_path()
        unzip_path = self.__get_unzip_path()
        if not os.path.exists(unzip_path):
            os.mkdir(unzip_path)
        with zipfile.ZipFile(write_path, 'r') as zip_ref:
            zip_ref.extractall(unzip_path)
        old_parent_dir_name = os.path.join(unzip_path, os.listdir(unzip_path)[0])
        new_parent_dir_name = self.__get_parent_download_path()
        os.rename(old_parent_dir_name, new_parent_dir_name)

    def perform_update(self):
        parent_dir_name = self.__get_parent_download_path()
        # walk through downloaded directory
        
        # for each update-capable file, replace (if exists) or add its equivalent in the live project
        return

    def clear_update_zone(self):
        shutil.rmtree(self.__get_save_path())
