# move

## Part of the senzing-tools suite.

This command can run stand-alone or as part of [senzing-tools](https://github.com/roncewind/senzing-tools).

Stand-alone syntax:

```console
move --input-url "https://public-read-access.s3.amazonaws.com/TestDataSets/SenzingTruthSet/truth-set-3.0.0.jsonl" --output-url "amqp://guest:guest@192.168.6.96:5672?exchange=senzing-rabbitmq-exchange&queue-name=senzing-rabbitmq-queue&routing-key=senzing.records"
```

as part of senzing-tools it is a sub-command:
```console
senzing-tools move --input-url "https://public-read-access.s3.amazonaws.com/TestDataSets/SenzingTruthSet/truth-set-3.0.0.jsonl" --output-url "amqp://guest:guest@192.168.6.96:5672?exchange=senzing-rabbitmq-exchange&queue-name=senzing-rabbitmq-queue&routing-key=senzing.records"
```

```
go run . --input-url "file:///home/roncewind/data/truth-set-3.0.0.jsonl" --output-url "amqp://user:bitnami@senzing-rabbitmq:5672?exchange=senzing-rabbitmq-exchange&queue-name=senzing-rabbitmq-queue&routing-key=senzing.records"
```

### comparison of different buffer and worker settings

| read chan buf	| workers	|time to move 1M (s) |
|--------------:|----------:|-------------------:|
|         10	|20 	    | 14                 |
|         100	|20 	    | 14                 |
|         10	|10 	    | 15                 |
|         50	|100	    | 15                 |
|         0	    |20 	    | 18                 |
|         100	|1000	    | 19                 |
|         10	|4  	    | 21                 |

