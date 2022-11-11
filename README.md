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
$ python -m sqs-dispatch --queue $SQS_QUEUE_URL --region $AWS_REGION
```