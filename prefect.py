import os
import time
import requests
import boto3
from typing import List, Dict, Any, Optional, Tuple
from prefect import flow, task, get_run_logger
import json

# scatter API endpoint
API_ENDPOINT = (
    "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/xxe9ff"
)

# my queue
MY_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-xxe9ff"

# Submit queue for final solution
SUBMIT_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"

sqs = boto3.client("sqs")


@task(retries=3, retry_delay_seconds=10)
def populate_queue() -> str:
    """Task 1: Populate your SQS queue with 21 messages via API call."""
    logger = get_run_logger()

    logger.info(f"Calling API to populate queue: {API_ENDPOINT}")
    response = requests.post(API_ENDPOINT)  # post request
    response.raise_for_status()

    payload = response.json()
    logger.info(f"API response: {payload}")

    # The API returns the SQS URL as confirmation
    sqs_url = payload.get("sqs_url")
    if sqs_url:
        logger.info(f"Queue populated successfully: {sqs_url}")
    else:
        logger.warning("No SQS URL returned from API")

    return sqs_url or MY_QUEUE_URL


@task(retries=3, retry_delay_seconds=10)
def get_queue_attributes(sqs_url: str) -> Dict[str, int]:
    """Get queue attributes to monitor message counts."""
    attrs = sqs.get_queue_attributes(
        QueueUrl=sqs_url, AttributeNames=["All"]
    ).get(  # get the attributes from sqs_url
        "Attributes", {}
    )
    keys = [  # proper keys
        "ApproximateNumberOfMessages",
        "ApproximateNumberOfMessagesNotVisible",
        "ApproximateNumberOfMessagesDelayed",
    ]
    return {k: int(attrs.get(k, "0")) for k in keys}


def _parse_word_message(msg: Dict[str, Any]) -> Tuple[int, str]:
    attrs = msg.get("MessageAttributes") or {}
    # normalize attribute keys to lowercase
    str_attrs = {
        (k or "").lower(): (v.get("StringValue") if isinstance(v, dict) else None)
        for k, v in attrs.items()
    }
    # populate body with messages
    body = (msg.get("Body") or "").strip()

    # Try to parse JSON body if present
    body_json = None
    if body.startswith("{") and body.endswith("}"):
        try:
            body_json = json.loads(body)
        except Exception:
            body_json = None

    # ORDER candidates (first hit wins)
    order_raw = (
        str_attrs.get("order_no")
        or str_attrs.get("order")
        or str_attrs.get("index")
        or str_attrs.get("position")
        or (
            body_json
            and (
                body_json.get("order_no")
                or body_json.get("order")
                or body_json.get("index")
            )
        )
    )
    if order_raw is None:  # if mising
        raise ValueError(f"Missing order attribute; attrs={attrs}, body={body!r}")

    order_no = int(str(order_raw))

    # WORD candidates (first hit wins)
    word = (
        str_attrs.get("word")
        or str_attrs.get("message")
        or str_attrs.get("phrase")
        or (
            body_json
            and (
                body_json.get("word")
                or body_json.get("message")
                or body_json.get("phrase")
            )
        )
        or body
    )
    word = (word or "").strip()
    # error handling
    if not word:
        raise ValueError(
            f"Empty word for order_no={order_no}; attrs={attrs}, body={body!r}"
        )

    return order_no, word


@task(retries=3, retry_delay_seconds=10)
def collect_all_messages(sqs_url: str, expected: int = 21) -> List[Tuple[int, str]]:
    """
    Task 2: Collect all word messages from your dedicated queue.
    Long-poll the queue until we collect all expected messages.
    """
    logger = get_run_logger()
    pairs: List[Tuple[int, str]] = []
    empty_polls = 0
    max_polls = 10  # limit on empty polls

    while len(pairs) < expected:
        resp = sqs.receive_message(
            QueueUrl=sqs_url,
            AttributeNames=["All"],  # gather all
            MessageAttributeNames=["All"],
            MaxNumberOfMessages=10,  # SQS max
            VisibilityTimeout=30,
            WaitTimeSeconds=10,  # long poll
        )
        msgs = resp.get("Messages", [])  # get messages

        """
        If no messages, increment empty poll count and check if we should continue
        """
        if not msgs:
            empty_polls += 1
            counts = get_queue_attributes.submit(sqs_url)
            counts = counts.result()
            remaining = sum(counts.values())
            logger.info(
                f"No messages yet. Counts={counts}, collected={len(pairs)}/{expected}, empty_polls={empty_polls}/{max_polls}"
            )

            # If there appear to be more coming (delayed/not visible), wait a bit; else break
            if remaining == 0 and empty_polls >= 3:
                logger.warning("Queue shows 0 remaining; stopping early.")
                break
            time.sleep(5)
            continue

        empty_polls = 0
        # process the recieved messages
        for m in msgs:
            try:
                pair = _parse_word_message(m)
                pairs.append(pair)
                logger.info(f"Collected word {pair[0]}: '{pair[1]}'")
            except Exception as e:
                logger.error(f"Error parsing message: {e}")
                logger.error(f"Problematic message: {m}")
            finally:
                # Always delete after reading
                sqs.delete_message(QueueUrl=sqs_url, ReceiptHandle=m["ReceiptHandle"])
        # good to show progress
        logger.info(f"Collected {len(pairs)}/{expected} so far…")
        time.sleep(0.5)

    # show how many colleceted vs expected
    if len(pairs) < expected:
        logger.warning(f"Only collected {len(pairs)}/{expected} messages")

    return pairs


@task
def assemble_phrase(pairs: List[Tuple[int, str]]) -> str:
    """
    Task 3 (part 1): Sort by order_no and join words.
    """
    if not pairs:
        raise ValueError("No word pairs provided for assembly")

    # Sort by order_no and extract words
    words = [word for _, word in sorted(pairs, key=lambda t: t[0])]
    phrase = " ".join(words)

    return phrase


@task(retries=3, retry_delay_seconds=10)
def send_solution(uvaid: str, phrase: str, platform: str = "prefect") -> None:
    """Task 3 (part 2): Submit the assembled phrase to the submit queue."""
    logger = get_run_logger()

    try:
        response = sqs.send_message(
            QueueUrl=SUBMIT_QUEUE_URL,  # proper submit queue url
            MessageBody=phrase,  # send the phrase
            MessageAttributes={  # proper attributes for submission
                "uvaid": {"DataType": "String", "StringValue": uvaid},
                "phrase": {"DataType": "String", "StringValue": phrase},
                "platform": {"DataType": "String", "StringValue": platform},
            },
        )
        logger.info(f"Solution submitted successfully: {response}")
        print(f"Response: {response}")

    # error handling
    except Exception as e:
        logger.error(f"Error submitting solution: {e}")
        print(f"Error: {e}")
        raise e


@flow(name="dp2-prefect")
def main():
    """Main Prefect flow following the README instructions."""
    logger = get_run_logger()
    uvaid = "xxe9ff"

    # Task 1: Populate the queue
    logger.info("=== TASK 1: Populating queue ===")
    queue_url = populate_queue()

    # Task 2: Collect all word messages
    logger.info("=== TASK 2: Collecting word messages ===")
    pairs = collect_all_messages(queue_url, expected=21)
    logger.info(f"Collected {len(pairs)} word pairs")

    # Task 3: Assemble the phrase
    logger.info("=== TASK 3: Assembling phrase ===")
    phrase = assemble_phrase(pairs)
    logger.info(f"ASSEMBLED PHRASE: {phrase}")

    # Task 3: Submit the solution
    logger.info("=== TASK 3: Submitting solution ===")
    send_solution(uvaid, phrase, platform="prefect")

    logger.info("=== COMPLETE ===")


# run flow
if __name__ == "__main__":
    main()
