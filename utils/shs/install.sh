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