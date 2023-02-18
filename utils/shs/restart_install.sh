
# 重装系统以后的配置

# 新建root账户
sudo passwd root

# 允许root远程访问
sudo vim /etc/ssh/sshd_config # PermitRootLogin yes
sudo systemctl restart sshd #重启ssh服务：

# 初始化conda
conda init

# 添加源
rapidsai: https://mirrors.aliyun.com/anaconda/cloud
nvidia: https://mirrors.sustech.edu.cn/anaconda-extra/cloud

# 安装Rapids
conda create -n Rapids -c rapidsai -c conda-forge -c nvidia  rapids=22.12 python=3.9 cudatoolkit=11.2 tensorflow
conda install -c conda-forge ipykernel

# 安装btop
sudo apt update
apt upgrade
snap install btop

# 删除所有pypi包
conda list | awk '/pypi/ {print $1}' | xargs pip uninstall -y

# 删除所有 /home notebook jupyter ipython

# 安装jupyterhub
conda create -n JupyterHub python=3.9
conda install -c conda-forge jupyterhub jupyterlab
jupyterhub --generate-config

# 配置jupyterlab
conda install -c conda-forge jupyterlab-language-pack-zh-CN
jupyter-lab --generate-config
c.ServerApp.root_dir = '/home/ubuntu/notebooks' # /home/ubuntu/.jupyter 配置在用户下面

# 配置kernels
/usr/local/miniconda3/envs/JupyterHub/share/jupyter/kernels

# 快照点 1 (Rapids)

# 永久启动jupyterhub
conda activate JupyterHub
nohup jupyterhub > /home/ubuntu/logs/jupyterhub.log 2>&1 &

# 安装pytorch
conda install pytorch torchvision torchaudio pytorch-cuda=11.6 -c pytorch -c nvidia

# 挂载硬盘
mount /dev/vdb /data

# 开机自动挂载磁盘
https://cloud.tencent.com/document/product/362/6734

# 新建软连接
ln -s /data/DataSets  /home/ubuntu/notebooks/DataSets

# 安装webui
https://github.com/AUTOMATIC1111/stable-diffusion-webui
apt install wget git python3 python3-venv
bash <(wget -qO- https://raw.githubusercontent.com/AUTOMATIC1111/stable-diffusion-webui/master/webui.sh)
ln -s /data/Model/Stable-diffusion  /home/ubuntu/stable-diffusion-webui/models/Stable-diffusion

source /home/ubuntu/stable-diffusion-webui/venv/bin/activate #激活虚拟环境
# (Optional) Makes the build much faster
pip install ninja
# Set TORCH_CUDA_ARCH_LIST if running and building on different GPU types
pip install -v -U git+https://github.com/facebookresearch/xformers.git@main#egg=xformers

# (this can take dozens of minutes)
pip install triton==2.0.0.dev20221120
export COMMANDLINE_ARGS="--listen --xformers --enable-insecure-extension-access"
#eta (noise multiplier) for ancestral samplers=0.68


# 安装Stata
cd /tmp/ && mkdir statafiles && cd statafiles
tar -zxf /data/Downloads/Stata17Linux64.tar.gz
cd /usr/local && sudo mkdir stata17 && cd stata17
sudo /tmp/statafiles/install

# 安装label
conda create --name Label-Studio python=3.9
conda activate Label-Studio
pip install label-studio

export LOCAL_FILES_SERVING_ENABLED=true
export LABEL_STUDIO_LOCAL_FILES_SERVING_ENABLED=true
export LABEL_STUDIO_LOCAL_FILES_DOCUMENT_ROOT=/data/DataSets/Label_Studio_DS
nohup label-studio start -p 9000 > /home/ubuntu/logs/label_studio.log 2>&1 &
ps -aux | grep label-studio