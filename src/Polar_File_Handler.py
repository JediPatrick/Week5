from Downloader import Downloader
import os
from pathlib import Path
import threading
from typing import Optional
from queue import Queue
import polars as pl
from xlsxwriter import Workbook


# Class for handling a file with download links
class FileHandler(object):

    # Creates an instance of the filehandler with a dictionary of successfull downloads
    def __init__(
        self,
        url_file: str,
        meta_file: str,
        destination: str,
        number_of_threads: Optional[int] = 10,
    ) -> None:
        self.number_of_threads = number_of_threads
        self.url_file = url_file
        self.meta_file = meta_file
        self.destination = destination

        self.download_status_list = []
        self.list_lock = threading.RLock()
        self.ID = "BRnum"

    def add_download_status(self, item):
        with self.list_lock:  # Lock can be acquired multiple times by the same thread
            self.download_status_list.append(item)

    # Function that starts a download instance using the downloader class. Used in threads
    def thread_downloader(self, queue: Queue) -> None:
        while not queue.empty():

            link, destination, name, alt_link = queue.get()

            downloader = Downloader()
            downloaded = downloader.download_handling(
                url=link,
                destination_path=os.path.join(destination, name + ".pdf"),
                alt_url=alt_link,
            )

            if downloaded:
                state = "yes"
            else:
                state = "no"
            self.add_download_status((name, state))
            queue.task_done()

    def import_data_files(self):
        file_data = pl.read_excel(source=self.url_file, columns=["BRnum", "Pdf_URL", "Report Html Address"])

        # Initiates empty dataframe
        meta_data = pl.DataFrame()
        # Tries reading the files listed as not downloaded
        if os.path.exists(self.meta_file):
            meta_data = pl.read_excel(self.meta_file, columns=[self.ID, "pdf_downloaded"])
            meta_data = meta_data.filter(pl.col("pdf_downloaded") == "yes")
            # Sort out files that are downloaded
            file_data = file_data.join(meta_data, on=self.ID, how="anti")

        return file_data, meta_data

    def thread_handler(self, file_data):
        queue = Queue()

        #    # counter to only download 20 files
        #    j = 0
        # We thru each br number and starts a download
        for row in file_data.rows(named=True):
            #        if j == 20:
            #            break
            alt_link = row["Report Html Address"]
            link = row["Pdf_URL"]
            index = row[self.ID]
            # Creates a new thread and adds them to the list so that we can make sure all downloads are done before exiting
            queue.put([link, self.destination, index, alt_link])
        #        j += 1
        # Makes sure each thread is done
        for i in range(self.number_of_threads):
            thread = threading.Thread(target=self.thread_downloader, args=(queue,))
            thread.start()

        queue.join()

    # Starts downlaoding files from urls listed in url_file which will be placed in the destination, and reported in the meta file
    def handler(self) -> None:
        # Tests if Path exist and if not creates directory
        Path(self.destination).mkdir(exist_ok=True)

        file_data, meta_data = self.import_data_files()
        if not file_data.is_empty():
            self.thread_handler(file_data)
            # Creates a dataframe from the dictionary of downloads
            finished_data_frame = pl.DataFrame(self.download_status_list, schema=[self.ID, "pdf_downloaded"])

            if not meta_data.is_empty():
                finished_data_frame = pl.concat([finished_data_frame, meta_data], rechunk=True)
            with Workbook(self.meta_file) as file:
                finished_data_frame.write_excel(workbook=file)
