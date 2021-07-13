# how to run
1. from Nakadi root project directory: `./gradlew clean assemble`
2. from docker-arm directory: `docker-compose up -d --build`, give it a minute to start
3. from Nakadi root project directory: `./gradlew :acceptance-test:acceptanceTest`
4. once finished, from docker-arm directory: `docker-compose down`