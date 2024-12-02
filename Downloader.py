import requests
from typing import Optional

#Class for handling a single download. The function main functionality is to take an url and download it into the path
class Downloader(object):

    #uses a url link and a destination to download a file. Optionally one can use an alt url if applicaple
    #Returns success is file got downlaoded
    def download_handling(self, url : str, destination_path : str, alt_url : Optional[str] = None) -> bool:

        success = False
        if not url and not alt_url:
            return success
        #Tries downloading with the main url
        fileDownloaded, response = self.download(url)
        
        #If it fails to download try the alternative url
        if not fileDownloaded and alt_url:
           fileDownloaded, response = self.download(alt_url)
        
        #Sace file to the distination if the download was success
        if fileDownloaded:
            success = self.save_to_file(destination_path,response)
       
        return success

    def save_to_file(self, destination_path : str, response):
         with open(destination_path, "wb") as file:
            try:
                file.write(response.content)
                return True
            except:
                return False

    def download(self, url : str):
        try:
            response = requests.get(url,stream = True, timeout=30)
            #Checks if the response was a pdf file
            if "application/pdf" in response.headers.get("content-type"):
                return True, response
            return False, None
        except:
            return False, None 

if __name__ == "__main__":
    downloader = Downloader()
    downloader.download_handling("http://arpeissig.at/wp-content/uploads/2016/02/D7_NHB_ARP_Final_2.pdf", "test.pdf")
