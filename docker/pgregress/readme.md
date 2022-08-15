docker-compose up -d --remove-orphans --build router shard1 shard2
docker-compose up --build --entrypoint=/bin/bash pgregress