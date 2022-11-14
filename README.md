# SQS Dispatch Worker

A very simple worker that executes commands from an SQS queue

## Message Structure

```json
{
  "command": ["echo", "'Hello World!'"]
}
```

## Run sqs-dispatch-worker

```bash
# To process messages from the queue
$ python -m sqs-dispatch --queue $SQS_QUEUE_URL --process

# To process messages from the queue
$ python -m sqs-dispatch --queue $SQS_QUEUE_URL --enqueue "ENTER_COMMAND_HERE"
```