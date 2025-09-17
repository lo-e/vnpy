:: -i https://pypi.tuna.tsinghua.edu.cn/simple

:: Upgrade pip & setuptools
python -m pip install --upgrade pip
python -m pip install --upgrade setuptools（可能会出现安装报错，可省略，或者更新验证版本setuptools==68.0.0）

::Install Python Modules
python -m pip install -r requirements.txt

:: Install vn.py
python -m pip install .