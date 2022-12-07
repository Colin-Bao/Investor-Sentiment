#
conda activate Investor-Sentiment
conda install -c conda-forge jupyterhub

#重启mysql
mysql -p
restart

#开启APP
conda activate jupyterhub_env
nohup jupyterhub > /home/logs/jupyterhub.log 2>&1 &
# ps -aux | grep jupyterhub

conda create --name label-studio python=3.9
conda activate label-studio
pip install label-studio

export LOCAL_FILES_SERVING_ENABLED=true
export LABEL_STUDIO_LOCAL_FILES_SERVING_ENABLED=true
export LABEL_STUDIO_LOCAL_FILES_DOCUMENT_ROOT=/data/DataSets/label_studio_ds


nohup label-studio start -p 9000 > /home/logs/label_studio.log 2>&1 &
ps -aux | grep label-studio