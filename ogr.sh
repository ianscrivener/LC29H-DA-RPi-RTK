ogr2ogr -f FlatGeobuf FlatGeobuf.fgb _LOGS_RAW/test_target.csv \
  -oo X_POSSIBLE_NAMES=longitude,lng,lon \
  -oo Y_POSSIBLE_NAMES=latitude,lat \
  -oo KEEP_GEOM_COLUMNS=NO