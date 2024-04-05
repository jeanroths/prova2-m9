# Producer e Consumer Kafka em Go
Este projeto contém um producer e consumer Kafka simples implementado em Go. O produtor envia mensagens para um tópico Kafka e o consumer lê e imprime as mensagens.

## Producer

O producer envia 100 mensagens para o tópico "qualidadeAr". Cada mensagem contém um tipo aleatório de poluente e um nível aleatório. As mensagens são enviadas como strings JSON.

## Consumer

O consumer lê mensagens do tópico Kafka e as imprime no console. O consumer usa o ID do grupo "go-consumer-group" e começa a consumir mensagens a partir do deslocamento mais antigo.

## Executando o código

Para executar o código, você precisa ter um cluster Kafka em execução em sua máquina local. Você pode usar os seguintes comandos para iniciar um cluster Kafka usando Docker:

```
docker-compose up -d
``` 

Depois de iniciar o cluster Kafka, você pode executar o producer e o consumer usando os seguintes comandos:

```
go run main.go
go run consumer.go
```

## Funcionamento

https://github.com/jeanroths/prova2-m9/assets/99195775/abd59b6b-b7de-4808-b3da-7ad39e63c140
