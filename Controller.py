from Polar_File_Handler import FileHandler
from typing import Optional
import os
import argparse

#Class for instantiating a download from a file into a given path. 
class Controller(object):

    #Initiates the controller class with default values
    def __init__(self) -> None:
        self.url_file_name = os.path.join("customer_data","GRI_2017_2020.xlsx")
        self.report_file_name = os.path.join("customer_data","Metadata2017_2020.xlsx")
        self.destination = "files"
    
    #Runs the filehandler
    def run(self):
        file_handler = FileHandler(self.url_file_name,self.report_file_name,self.destination,1)
        file_handler.handler()

#My main function
if __name__ == "__main__":
    controller = Controller()
    controller.run()
