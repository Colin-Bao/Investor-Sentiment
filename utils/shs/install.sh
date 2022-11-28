#
conda activate Investor-Sentiment
conda install -c conda-forge pyarrow

#重启mysql
mysql -p
restart

#开启APP
conda activate Investor-Sentiment
nohup jupyterhub > /home/Logs/jupyterhub.log 2>&1 &
# ps -aux | grep jupyterhub.sh