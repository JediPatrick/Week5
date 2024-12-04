from Polar_File_Handler import FileHandler
from typing import Optional
import os
import argparse


# Class for instantiating a download from a file into a given path.
class Controller(object):

    # Initiates the controller class with default values
    def __init__(self) -> None:
        parent_folder = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

        # Create a path for the "files" folder in the parent folder
        files_folder = os.path.join(parent_folder, "files")
        data_folder = os.path.join(parent_folder, "customer_data")
        self.url_file_name = os.path.join(data_folder, "GRI_2017_2020.xlsx")
        self.report_file_name = os.path.join(data_folder, "Metadata2017_2020.xlsx")
        self.destination = files_folder

    # Runs the filehandler
    def run(self):

        file_handler = FileHandler(self.url_file_name, self.report_file_name, self.destination, 1)
        file_handler.handler()


# My main function
if __name__ == "__main__":
    controller = Controller()
    controller.run()
