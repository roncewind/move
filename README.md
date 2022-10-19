# move

## Part of the senzing-tools suite.

This command can run stand-alone or as part of [senzing-tools](https://github.com/roncewind/senzing-tools).

Stand-alone syntax:

```console
move --inputURL "https://public-read-access.s3.amazonaws.com/TestDataSets/SenzingTruthSet/truth-set-3.0.0.jsonl" --outputURL "amqp://guest:guest@192.168.6.96:5672"
```

as part of senzing-tools it is a sub-command:
```console
senzing-tools move --inputURL "https://public-read-access.s3.amazonaws.com/TestDataSets/SenzingTruthSet/truth-set-3.0.0.jsonl" --outputURL "amqp://guest:guest@192.168.6.96:5672"
```
