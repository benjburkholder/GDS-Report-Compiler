"""
Marketing Data Customizer Variant

This Customizer class enables a few key administrative functions:
    - post processing on the marketing_data table
    - (todo) marketing_data table / column / index management
    - (todo) marketing data auditing (view Test variant (see /tests))

"""
# STANDARD IMPORTS
import os
import pathlib

# CUSTOM IMPORTS
from ..core import Customizer


def execute_post_processing_scripts_for_process(script_filter: str = None):
    md = MarketingData()
    scripts = md.find_post_processing_scripts(script_filter=script_filter)
    if scripts:
        scripts = md.sort_script_order(scripts=scripts)
        md.execute_scripts(scripts=scripts)
    else:
        if script_filter:
            print(f'INFO: No post processing scripts with script_filter {script_filter} found.')
        else:
            print('INFO: No scripts found for post processing.')
    return True


def get_file_content(directory: str, file: str) -> str:
    """
    Utility static function for generating file content as a string based on directory and file name
    ====================================================================================================
    :param directory:
    :param file:
    :return:
    """
    full_path = os.path.join(
        directory,
        file
    )
    with open(full_path, 'r') as query:
        return query.read()


class MarketingData(Customizer):

    __utils_path_idx = 1

    def __get_utils_path(self) -> str:
        """
        Utility function to help us get back down to the GRC utils path
        ====================================================================================================
        :return:
        """
        return pathlib.Path(
            os.path.dirname(
                os.path.abspath(__file__)
            )
        ).parents[self.__utils_path_idx]

    def get_user_scripts_directory(self, script_dir: str) -> str:
        """
        Get the directory for user-defined scripts based on the given script_dir
        ====================================================================================================
        :param script_dir:
        :return:
        """
        return os.path.join(
            self.__get_utils_path(),
            'scripts',
            script_dir,
            'user'
        )

    # only accept SQL scripts
    valid_script_file_extension = '.sql'

    # where to look for post processing scripts? official static dir name
    post_processing_dir = 'post_processing'

    def find_post_processing_scripts(self, script_filter: str = None) -> list:
        """
        Looks in the user's scripts directory, and extracts all queries
        Queries are returned as a list of SQL strings

        Use inc_val to filter scripts that are only relevant for the processing at hand
        OR leave it blank to simply return all post_processing scripts
        ====================================================================================================
        :param script_filter: if provided, filter the returned scripts based on this value
        :return:
        """
        queries = []
        user_scripts_directory = self.get_user_scripts_directory(
            script_dir=self.post_processing_dir
        )
        script_files = os.listdir(user_scripts_directory)
        for file in script_files:
            if self.valid_script_file_extension in file:
                if script_filter:
                    if script_filter in file:
                        execution_order_number = int(file.split('_')[0])
                        assert type(execution_order_number) == int
                        file_content = get_file_content(
                            directory=user_scripts_directory,
                            file=file
                        )
                        script = (execution_order_number, file_content)

                        queries.append(
                            script
                        )
                else:
                    queries.append(
                        get_file_content(
                            directory=user_scripts_directory,
                            file=file
                        )
                    )
        return queries

    def execute_scripts(self, scripts: list) -> bool:
        """
        Executes each SQL script provided and DOES NOT return results of any kind
        Returns True to signify success
        ====================================================================================================
        :param scripts:
        :return:
        """
        for script in scripts:
            with self.engine.begin() as con:
                print('********************************************************')
                print(script[1])
                con.execute(script[1])
                print('********************************************************')
        return True

    @staticmethod
    def sort_script_order(scripts: list) -> list:
        """
        Sorts list from 1 to n based for execution order.
        :param scripts:
        :return: list
        """
        sorted_list = sorted(
            scripts, key=lambda x: x[0]
        )

        return sorted_list


