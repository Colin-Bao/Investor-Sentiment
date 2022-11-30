# 创建下载文件夹
mkdir /home/Downloads
cd /home/Downloads

# 安装Anconda
wget https://repo.anaconda.com/archive/Anaconda3-2022.10-Linux-x86_64.sh
bash /home/Downloads/Anaconda3-2022.10-Linux-x86_64.sh
# 安装目录在/root/anaconda3
conda config --set auto_activate_base true

# 安装Stata
cd /tmp/ && mkdir statafiles && cd statafiles
tar -zxf /data/Downloads/Stata17Linux64.tar.gz
cd /usr/local && mkdir stata17 && cd stata17
/tmp/statafiles/install

# 安装glibc libncurses5
conda install -c rmg glibc
apt install libncurses5

# 激活并运行
#501709301094!$n1dp$aca97zqi8vi0k3n6p8it3dnw29eef82ri!snic!Colin's Stata!!-2285!
./stinit
cd /usr/local/stata17
./stata

# 安装jupyterhub
conda install -c conda-forge nodejs npm
conda install -c conda-forge pandas numpy matplotlib
conda install -c conda-forge matplotlib
# curl -L https://tljh.jupyter.org/bootstrap.py | sudo -E python3 - --admin root

# **删除索引缓存、锁定文件、未使用过的包和tar包。**
conda clean -a

# 创建虚拟环境
conda create -n Investor-Sentiment python=3.9
sudo chmod a+w .conda



conda activate Investor-Sentiment
conda install -c conda-forge jupyterhub  jupyterlab notebook numpy pandas sqlalchemy mysql-connector-python 

# conda install jupyterlab notebook  # needed if running the notebook servers in the same environment

jupyterhub --generate-config
# /root/jupyterhub_config.py
jupyter lab --generate-config
# /root/.jupyter/jupyter_lab_config.py
c.ServerApp.allow_root = True
c.JupyterHub.admin_access = True
c.Authenticator.admin_users = {'root'}
c.LocalAuthenticator.create_system_users = True
allowed_users={'colin'}
# 授权给envs文件夹
ll -a /root/anaconda3/envs
# passwd colin
# 将目前目录下的所有档案与子目录皆设为任何人可读取
chmod a+r+x -R /root
chmod a+r+w+x -R /root/anaconda3/envs
# 文件夹和子文件夹

cd /root/anaconda3/envs/Investor-Sentiment

# 多用户搞定


# 配置软件包 https://jupyterhub.readthedocs.io/en/stable/reference/config-user-env.html
ipython profile create
jupyter server --generate-config

# 安装lab拓展
# 配置中文
conda activate Investor-Sentiment
conda install -c conda-forge jupyterlab-language-pack-zh-CN
conda install -c conda-forge nodejs
conda install -c conda-forge jupyterlab_execute_time

# 1.labextension
# jupyter labextension install
conda install -c conda-forge nbresuse
# 全局拓展
conda install -c conda-forge jupyter-resource-usage
jupyter labextension list
jupyter nbextension list
# c.Spawner.mem_limit = 4*1024*1024*1024
# c.Spawner.cpu_limit = 4
c.ResourceUseDisplay.track_cpu_percent = True
jupyterlab-topbar-extension
moniter
jupyter lab --ResourceUseDisplay.track_cpu_percent=True
# 要把配置文件放到每个用户下面