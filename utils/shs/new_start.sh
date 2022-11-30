
#
conda clean -i
# 换源
# https://mirrors.tuna.tsinghua.edu.cn/help/anaconda/

# https://rapids.ai/start.html
conda install -c rapidsai -c conda-forge -c nvidia  \
    cuml=22.10 cudf=22.10  python=3.9 cudatoolkit=11.2 \
    tensorflow
#创建一个base环境,用于运行jupyterhub


#卸载旧的环境
conda env remove -n jupyterhub_env
conda env list
conda activate  Investor-Sentiment
conda install -c conda-forge cupy
conda create -n test_temp -c conda-forge python=3.9
conda activate  Investor-Sentiment

# 配置新的虚拟环境
conda create -n base_env_0 python=3.9
conda create -n jupyterhub_env python=3.9
conda create -n Investor-Sentiment python=3.9
conda activate jupyterhub_env
conda install mamba -c conda-forge
conda  install nodejs  -c conda-forge
conda install -c conda-forge jupyterhub
conda install -c conda-forge jupyterlab notebook
conda  install jupyterhub jupyterlab notebook ipykernel ipython -c conda-forge
#
conda  install   ipykernel ipython -c conda-forge
#安装太慢
conda config --remove channels conda-forge
conda config --add channels conda-forge
conda update conda
#配置jupyterhub jupyterlab
conda install notebook ipykernel ipython -c conda-forge
conda install notebook ipykernel ipython -c conda-forge
ipython kernel install --name "Investor-Sentiment" --user
jupyter kernelspec list
ipython kernel install --name "jupyterhub_env" --user
python -m ipykernel install --user --name Investor-Sentiment --display-name "Investor-Sentiment"
jupyter kernelspec remove python3

python -m ipykernel install --user --name jupyterhub_env --display-name "jupyterhub_env"
conda create -n Stata --clone base_env_0