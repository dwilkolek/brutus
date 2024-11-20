cd snowflake-monitor
npm run build-image
cd ..
docker-compose --env-file .env up
