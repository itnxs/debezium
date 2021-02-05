# debezium-server
使用Golang处理debezium数据同步,支持mysql和postgres同步到mysql,postgres,elasticsearch,clickhouse

## 运行
```
docker-compose up
go run cmd/debezium-server/main.go -conf=./etc/config.toml
```

## 注册
```console
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" localhost:8083/connectors/ -d @source-mysql.json
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" localhost:8083/connectors/ -d @source-postgres.json
```

## 删除
```console
curl -i -X DELETE localhost:8083/connectors/mysql-source-connector 
curl -i -X DELETE localhost:8083/connectors/postgres-source-connector
```

## ES
```console
curl localhost:9200/_cat/indices
curl localhost:9200/_cat/count/users
curl localhost:9200/users/_search
```