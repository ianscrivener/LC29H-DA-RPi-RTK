gpq convert \
    '_LOGS_RAW/20250711_065025_rtk_logcopy.geojson' \
    '_LOGS_RAW/20250711_065025_rtk_logcopy.parquet' \
    --compression="zstd" \
    --from="geojson" \
    --to="geoparquet"