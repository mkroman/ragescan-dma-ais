import ftplib
import logging

DMA_AIS_FTP_HOST = 'ftp.ais.dk'

class FTP:
  """Provides a convenient wrapper around the FTP server where DMA publishes their files"""

  def __init__(self):
    """Constructs a new FTP wrapper"""
    self.connected = False
    self.logger = logging.getLogger(__name__)

  def connect(self):
    """Connects to the FTP server.

    This is usually not necessary to do manually - any other method will connect if not already
    connected.
    """
    if not self.connected:
      self.logger.debug(f"Connecting to ftp://{DMA_AIS_FTP_HOST}")
      self.ftp = ftplib.FTP(DMA_AIS_FTP_HOST)
      self.logger.debug("Logging in as anonymous")
      self.ftp.login()
      self.connected = True

  def cd(self, path):
    """Sets the current directory"""
    if not self.connected:
      self.connect()

    self.logger.debug(f"Changing directory to {path}")
    self.ftp.cwd(path)

  def ls(self, path=""):
    """Returns a list of files in the given path on the remote server"""
    if not self.connected:
      self.connect()

    return self.ftp.nlst(path)

  def download(self, file_name, callback):
    """Downloads the file with the given file_name and calls the given callback for each block
    downloaded
    """
    if not self.connected:
      self.connect()

    self.logger.debug(f"Downloading file {file_name}")
    self.ftp.retrbinary(f"RETR {file_name}", callback)

