from dataservice import TurtleCryptoDataDownloading

if __name__ == '__main__':
    """
    contract_list = ['okef/btc.usd.q', 'okef/eth.usd.q', 'okef/eos.usd.q', 'okswap/btc.usd.td', 'okswap/eth.usd.td',
                    'okswap/eos.usd.td', 'okex/btc.usdt', 'okex/eth.usdt', 'okex/eos.usdt']
    dataDownload = TurtleCryptoDataDownloading()
    dataDownload.download_from_onetoken(contract_list=contract_list, days=1)
    result, complete_msg, back_msg, lost_msg = dataDownload.generate_for_onetoken(contract_list=contract_list, days=10)
    print('\n\n' + lost_msg + back_msg)
    """

    #"""
    contract_list = ['BTCUSD', 'ETHUSD', 'EOSUSD']
    dataDownload = TurtleCryptoDataDownloading()
    dataDownload.download_from_bybit(contract_list=contract_list, days=1)
    result, complete_msg, back_msg, lost_msg = dataDownload.generate_for_bybit(contract_list=contract_list, days=1)
    print('\n\n' + lost_msg + back_msg)
    #"""