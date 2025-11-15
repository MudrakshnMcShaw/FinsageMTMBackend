cd "/root/FinsageMTMBackend" || exit
/usr/local/bin/pm2 delete "Finsage mtm Backend"
/usr/local/bin/pm2 start main.py --name "Finsage mtm Backend" --interpreter /root/venv/bin/python3.10 --no-autorestart