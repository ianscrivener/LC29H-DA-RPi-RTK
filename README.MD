# RKT Corrections for LC29H(DA)


## Install
```
sudo pip install -r requirements.txt
```


## SetUp systemd

```
chmod +x /home/pi/rtk/main.py
sudo nano /etc/systemd/system/rtk-python.service
```

**Paste the following code**

```
[Unit]
Description=GNSS RTK
After=network.target
Wants=network.target

[Service]
Type=simple
User=pi 
Group=pi 
WorkingDirectory=/home/pi/rtk
ExecStart=/usr/bin/python3 /home/pi/rtk/main.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

# Allow access to serial ports
SupplementaryGroups=dialout

[Install]
WantedBy=multi-user.target

```

**Enable and start the systmd service**

```bash
# Reload systemd to read new service
sudo systemctl daemon-reload

# Enable service to start at boot
sudo systemctl enable rtk-python.service

# Start the service now
sudo systemctl start rtk-python.service

# Check status
sudo systemctl status rtk-python.service

```



**Service Management Commands**

```
# Check service status
sudo systemctl status rtk-python

# View logs
sudo journalctl -u rtk-python -f

# View recent logs
sudo journalctl -u rtk-python --since today

# Stop service
sudo systemctl stop rtk-python

# Restart service
sudo systemctl restart rtk-python

# Disable auto-start
sudo systemctl disable rtk-python

# Check if enabled
sudo systemctl is-enabled rtk-python
```