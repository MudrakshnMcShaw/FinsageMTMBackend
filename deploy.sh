# Modified lines in deploy.sh
pm2 delete "Finsage mtm Backend"
pm2 start main.py --name "Finsage mtm Backend" --interpreter /root/FinsageMTMBackend/venv/bin/python3.10 --no-autorestart
