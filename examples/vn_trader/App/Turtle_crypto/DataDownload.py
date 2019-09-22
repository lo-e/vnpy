from dataservice import TurtleCryptoDataDownloading

if __name__ == '__main__':
    contract_list = ['okef/btc.usd.q', 'okef/eth.usd.q', 'okef/eos.usd.q', 'okswap/btc.usd.td', 'okswap/eth.usd.td',
                    'okswap/eos.usd.td']
    dataDownload = TurtleCryptoDataDownloading()
    dataDownload.download(contract_list=contract_list)
    result, complete_msg, back_msg, lost_msg = dataDownload.generate(contract_list=contract_list)
    print('\n\n' + lost_msg + back_msg)