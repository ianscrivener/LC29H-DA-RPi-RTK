NTRIP_HOST=ntrip.data.gnss.ga.gov.au
NTRIP_PORT=2101
NTRIP_MOUNTPOINT=SPPT00AUS0
NTRIP_USERNAME=ianscrivener
NTRIP_PASSWORD=RZT5jab-qug0bpm1vah
NTRIP_USE_HTTPS=false
NTRIP_USER_AGENT=LC29HDA-Client

# Run this in your terminal
curl -v --user "$NTRIP_USERNAME:NTRIP_$PASSWORD" \
     "http://$NTRIP_HOST:$NTRIP_PORT/$NTRIP_MOUNTPOINT" \
     --header "Ntrip-Version: NTRIP/2.0" \
     --header "User-Agent: NTRIP curl" \
     --header "Connection: close"
