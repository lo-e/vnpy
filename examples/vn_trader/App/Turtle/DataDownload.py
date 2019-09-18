from dataservice import TurtleDataDownloading

if __name__ == '__main__':
    dataDownload = TurtleDataDownloading()
    result, msg = dataDownload.download()
    print(result)
    print(msg)