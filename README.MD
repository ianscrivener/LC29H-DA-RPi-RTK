# RKT Corrections for LC29H(DA)



## Install dependencies 
```
sudo apt install git python3-pip -y
```

## Install the python code 
```
cd ~/
git clone git@github.com:ianscrivener/LC29H-DA-RPi-RTK.git rtk
cd rtk

sudo pip install -r requirements.txt
```


## Setup systemd to run the python script as a Raspberry Pi service

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
# StandardOutput=journal

# Disable the python app console output 
StandardOutput=null   

# Though still log errors
StandardError=journal

# Log only errors
LogLevel=err

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



## Setup systemd to upload logs via S3

```
chmod +x /home/pi/rtk/s3_log_upload.py
sudo nano /etc/systemd/system/rtk-s3-upload-python.service
```

**Paste the following code**


```
[Unit]
Description=GNSS RTK S3 Upload
After=network.target
Wants=network.target

[Service]
Type=simple
User=pi
Group=pi
WorkingDirectory=/home/pi/rtk
ExecStart=/usr/bin/python3 /home/pi/rtk/s3_log_upload.py
Restart=always
RestartSec=10
# StandardOutput=journal

# Disable the python app console output 
StandardOutput=null

# Though still log errors
StandardError=journal

# Log only errors
LogLevel=err

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
sudo systemctl enable rtk-s3-upload-python.service

# Start the service now
sudo systemctl start rtk-s3-upload-python.service

# Check status
sudo systemctl status rtk-s3-upload-python.service

```



**Service Management Commands**

```
# Check service status
sudo systemctl status rtk-s3-upload-python.service

# View logs
sudo journalctl -u rtk-s3-upload-python.service -f

# View recent logs
sudo journalctl -u rtk-s3-upload-python.service --since today

# Stop service
sudo systemctl stop rtk-s3-upload-python.service

# Restart service
sudo systemctl restart rtk-s3-upload-python.service

# Disable auto-start
sudo systemctl disable rtk-s3-upload-python.service

# Check if enabled
sudo systemctl is-enabled rtk-s3-upload-python.service
```


--- 

### Testing

There are some node.js testing scripts in _node_testing 
