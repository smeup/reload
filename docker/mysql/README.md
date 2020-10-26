# Docker mysql 

With this docker-compose you can have a local mysql instance up and running 

- Prerequisite: docker installed with docker-compose extension

## Use 

1. Up the compose
```
    docker-compose up -d
```
2. Access phpmyadmin
```
    your_ip:8183
    Server: mysql
    Username: root
    Password: root
```
3. Access mysql 
```
  url: jdbc:mysql://localhost:3306/
  user: root
  password: root
```

