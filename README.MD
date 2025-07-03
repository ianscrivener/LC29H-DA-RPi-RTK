# RKT Corrections for LC29H(DA)


## Install
```
sudo pip install -r requirements.txt
```


## SetUp systemd

```
chmod +x /home/ians/rtk/app.py
sudo nano /etc/systemd/system/rtk-python.service
```

**Paste the foillowing code**
```
[Unit]
Description=GNSS RTK
After=network.target
Wants=network.target

[Service]
Type=simple
User=ians 
Group=ians 
WorkingDirectory=/home/ians/rtk
ExecStart=/usr/bin/python3 /home/ians/rtk/app.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

# Allow access to serial ports
SupplementaryGroups=dialout

[Install]
WantedBy=multi-user.target



```
