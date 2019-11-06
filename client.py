from ftplib import FTP

DOMAIN_NAME = 'ireknazm.space'

ftp = FTP(DOMAIN_NAME)
ftp.connect('localhost', 1026)
ftp.login()
ftp.cwd('directory_name')       # replace with your directory
ftp.retrlines('LIST')


def uploadFile():
    filename = 'testfile.txt'   # replace with your file in your home folder
    ftp.storbinary('STOR '+filename, open(filename, 'rb'))
    ftp.quit()


def downloadFile():
    filename = 'testfile.txt' # replace with your file in the directory ('directory_name')
    localfile = open(filename, 'wb')
    ftp.retrbinary('RETR ' + filename, localfile.write, 1024)
    ftp.quit()
    localfile.close()


# uploadFile()
# downloadFile()