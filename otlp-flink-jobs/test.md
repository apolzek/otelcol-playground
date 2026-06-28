telemetrygen traces  --traces  100000 --workers 4 --rate 1000 --service fake-service --otlp-insecure --otlp-endpoint localhost:4317 &
telemetrygen metrics --metrics 100000 --workers 4 --rate 1000 --service fake-service --otlp-insecure --otlp-endpoint localhost:4317 &
telemetrygen logs    --logs    100000 --workers 4 --rate 1000 --service fake-service --otlp-insecure --otlp-endpoint localhost:4317 &
wait