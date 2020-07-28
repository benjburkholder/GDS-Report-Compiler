"""
Queue Manager Module
"""
import os
import json
import pathlib


class QueueManager:
    """
    Determine which scripts we should run, and with which arguments
    """

    def get(self) -> dict:
        """
        Returns queue in normalized form for script running
        ====================================================================================================
        :return:
        """
        return self.get_config()

    project_root_idx = 1

    def __get_project_root(self):
        """
        Returns the global location of the project root
        ====================================================================================================
        :return:
        """
        return pathlib.Path(__file__).parents[self.project_root_idx]

    def get_config(self) -> dict:
        path = os.path.join(
            self.__get_project_root(),
            'conf',
            'stored',
            'workflow.json'
        )
        with open(path, 'r') as file:
            return json.load(file)
