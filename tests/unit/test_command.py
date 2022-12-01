from sqs_dispatch.command import execute
import pytest


@pytest.mark.asyncio
async def test_retain_ordering():
    output = []
    await execute(
        "echo stdout.1; >&2 echo strerr.1; echo stdout.2; >&2 echo strerr.2",
        callback=lambda x, y: output.append(y),
    )

    assert output[0] == "stdout.1"
    assert output[1] == "strerr.1"
    assert output[2] == "stdout.2"
    assert output[3] == "strerr.2"


@pytest.mark.asyncio
async def test_single_string():
    output = []
    await execute(
        "echo this is a test",
        callback=lambda x, y: output.append(y),
    )

    assert len(output) == 1
    assert output[0] == "this is a test"


@pytest.mark.asyncio
async def test_array_string():
    output = []
    await execute(
        ["echo", "this", "is", "a", "test"],
        callback=lambda x, y: output.append(y),
    )

    assert len(output) == 1
    assert output[0] == "this is a test"


@pytest.mark.asyncio
async def test_long_stderr():
    output = []
    await execute(
        [">&2 printf 'testing\n%.0s' {1..5}"],
        callback=lambda x, y: output.append(y),
    )

    assert len(output) == 5
