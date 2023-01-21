nohup label-studio start -p 9000 > /home/logs/label_studio.log 2>&1 &

nohup tensorboard --logdir=/data/Models/TWITTER_SENT_2015/logs --bind_all   > /home/logs/tensorboard.log 2>&1 &

ps -aux | grep tensorboard