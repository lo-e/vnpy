# VNPY环境搭建（Win）
> **Git安装与设置**

```
安装
https://git-scm.com/downloads
```

```
全局设置
git config --global user.name "your user name"
git config --global user.email "your user email"
```

```
创建ssh key
ssh-keygen -t rsa -C "your user email"
在.ssh目录下将id_rsa.pub内容添加到github
```

> **下载VNPY**

```
git@github.com:lo-e/vnpy.git
```

> **‌安装Visual Studio Commuity 2013或者只安装vc_redist**
（一般window自带相关库，这一步可省略，视情况而定）

```
方案一【安装Visual Studio Community 2013】：
移步百度网盘或https://www.visualstudio.com/zh-hans/downloads/下载

方案二【安装vc_redist】：
百度搜索Redistributable找到微软官方下载入口或直接https://www.microsoft.com/zh-CN/download/details.aspx?id=48145下载【推荐x86】
```

> **安装文本编辑器工具Sublime Text**

```
移步百度网盘
或者
http://www.sublimetext.com/2
```

> **安装Mongodb数据库**（推荐Windows 2008 plus（64bits）支持ssl协议的版本，使用complete默认配置安装）

```
移步百度网盘
或者
https://www.mongodb.com/download-center?jmp=tutorials#community
```
```
Mongodb注册为Windows系统服务，根据官方指导操作（当前官方提示最新版本安装过程自动完成注册Windows系统服务，这一步可省略，视情况而定）
https://docs.mongodb.com/manual/tutorial/install-mongodb-on-windows/#configure-a-windows-service-for-mongodb-community-edition
```

> **安装RoboMongo**

```
https://robomongo.org/
```

> **安装Anaconda**推荐

```
下载最新对应Pyhon 3.0以上版本
https://repo.continuum.io/archive/.winzip/ 
安装时注意将后面的添加为系统环境变量选项打勾
```

```
【推荐版本】
vnpy now_v3.0.0
Windows Server 2019 Datacenter
Anaconda3-2020.02-Windows-x86_64.zip    463.9M  2020-03-11 11:16:33 (Python 3.7.6)

vnpy now_v3.0.0
Windows Server 2019 Datacenter
Anaconda3-2021.11-Windows-x86_64.zip	507.5M	2021-11-17 12:10:52 (Python 3.9.7)
```

> **安装VNPY**

```
点击【vnpy\install.bat】文件，包含安装需要的第三方Pyhon库和其他准备内容
输入命令完成安装
pyhon -m pip install .
```

> **报错**

ta-lib 安装错误，则手动下载安装，而且需要下载对应Pyhon版本

```
https://www.lfd.uci.edu/~gohlke/pythonlibs/#ta-lib
https://github.com/cgohlke/talib-build/releases
例如：
cmd输入ipython显示Python版本为3.8.6
则pip install TA_Lib‑0.4.18‑cp38‑cp38‑win_amd64.whl
```

DLL load failed......

```
方案一：
重装anaconda,选择64位版本，并且最好不要选择最新的anaconda

方案二：
回到【安装Visual Studio Commuity 2013或者只安装vc_redist】这一步骤筛查
```

rqdatac或者vnpy_rqdata安装出错

```
cmd输入ipython检查anaconda是否32位版本，重装64位anaconda
```

can't import name 'Qsci' from 'PyQt5'

```
重装QSintilla
```

requests.exceptions.ProxyError: HTTPSConnectionPool(host='api.telegram.org', port=443)

```
重装requests（建议版本requests==2.22.0）
```

ERROR: Failed building wheel for PyQt5-sip

error: Microsoft Visual C++ 14.0 or greater is required. Get it with "Microsoft C++ Build Tools": https://visualstudio.microsoft.com/visual-cpp-build-tools/
```
1、使用powershell安装C++生成工具
wget https://aka.ms/vs/17/release/vs_BuildTools.exe -o vs_BuildTools.exe ; cmd /c vs_BuildTools.exe

2、Visual Studio安装界面选择左边第一项"使用C++的桌面开发"，一定要勾选右边"C++/CLI支持

3、安装完成后启动V
```