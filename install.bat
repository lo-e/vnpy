:: Upgrade pip & setuptools
python -m pip install --upgrade pip setuptools

::Install rqdatac、talib、ibapi（requirements.txt可直接下载）
pip install --extra-index-url https://rquser:ricequant99@py.ricequant.com/simple/ rqdatac 
python -m pip install https://pip.vnpy.com/colletion/TA_Lib-0.4.17-cp37-cp37m-win_amd64.whl
::python -m pip install https://pip.vnpy.com/colletion/ibapi-9.75.1-001-py3-none-any.whl

::Install Python Modules
python -m pip install -r requirements.txt
::下载很慢的话通过更换清华源下载改善
::pip install -i https://pypi.tuna.tsinghua.edu.cn/simple -r requirements.txt

:: Install vn.py
::python -m pip install .