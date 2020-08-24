:: Upgrade pip & setuptools
python -m pip install -i https://pypi.tuna.tsinghua.edu.cn/simple --upgrade pip setuptools

::Install rqdatac、talib、ibapi（requirements.txt可直接下载）
pip install -i https://pypi.tuna.tsinghua.edu.cn/simple --extra-index-url https://rquser:ricequant99@py.ricequant.com/simple/ rqdatac 
python -m pip install -i https://pypi.tuna.tsinghua.edu.cn/simple https://pip.vnpy.com/colletion/TA_Lib-0.4.17-cp37-cp37m-win_amd64.whl
::python -m pip install -i https://pypi.tuna.tsinghua.edu.cn/simple  https://pip.vnpy.com/colletion/ibapi-9.75.1-001-py3-none-any.whl
python -m pip install -i https://pypi.tuna.tsinghua.edu.cn/simple https://pip.vnpy.com/colletion/quickfix-1.15.1-cp37-cp37m-win_amd64.whl

::Install Python Modules
python -m pip install -i https://pypi.tuna.tsinghua.edu.cn/simple -r requirements.txt

:: Install vn.py
::python -m pip install .