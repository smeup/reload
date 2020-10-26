# Docker mongodb 

With this docker-compose script you can have a local mongodb instance up and running.

- Prerequisite: docker installed with docker-compose extension

## Use 

1. Up the compose
```
    docker-compose up -d
```
2. Access mongodb (ie, using MongoDBCompass)
``` 
    mongodb://<your_IP>:27017/?readPreference=primary&appname=MongoDB%20Compass&ssl=false
```



