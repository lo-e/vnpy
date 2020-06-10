from dataservice import TurtleCryptoDataDownloading

if __name__ == '__main__':
    """
    contract_list = ['okef/btc.usd.q', 'okef/eth.usd.q', 'okef/eos.usd.q', 'okswap/btc.usd.td', 'okswap/eth.usd.td',
                    'okswap/eos.usd.td', 'okex/btc.usdt', 'okex/eth.usdt', 'okex/eos.usdt']
    contract_list = ['okef/btc.usd.t', 'okef/btc.usd.n']
    days = 20
    dataDownload = TurtleCryptoDataDownloading()
    dataDownload.download_from_onetoken(contract_list=contract_list, days=days)
    result, complete_msg, back_msg, lost_msg = dataDownload.generate_for_onetoken(contract_list=contract_list, days=days)
    print('\n\n' + lost_msg + back_msg)
    """

    #"""
    contract_list = ['BTCUSD', 'ETHUSD']
    days = 2
    dataDownload = TurtleCryptoDataDownloading()
    dataDownload.download_from_bybit(contract_list=contract_list, days=days)
    result, complete_msg, back_msg, lost_msg = dataDownload.generate_for_bybit(contract_list=contract_list, days=days)
    print('\n\n' + lost_msg + back_msg)
    #"""