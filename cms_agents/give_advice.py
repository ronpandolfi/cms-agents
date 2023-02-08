import datetime
import pprint
import uuid

from bluesky_kafka import Publisher, RemoteDispatcher
import nslsii.kafka_utils

from tiled.client import from_profile


def give_advice(bluesky_run):
    print(f"give advice on run {bluesky_run}")
    advice = {
        "next big thing": "avocado toast"
    }

    return advice


def publish_advice_documents(advice, advice_publisher):
    


def respond_to_stop_with_advice():

    kafka_config = nslsii.kafka_utils._read_bluesky_kafka_config_file(config_file_path="/etc/bluesky/kafka.yml")

    cms_tiled_client = from_profile("cms")

    advice_publisher = Publisher(
        topic="",
        bootstrap_servers=",".join(kafka_config["bootstrap_servers"]),
        producer_config=kafka_config["producer_config"]
    )

    def on_stop_give_advice(name, doc):
        print(
            f"{datetime.datetime.now().isoformat()} document: {name}\n"
            f"contents: {pprint.pformat(doc)}\n"
        )
        if name == "stop":
            # look up the results of this run
            run_start_id = doc["run_start"]
            print(f"found run_start id {run_start_id}")
            bluesky_run = cms_tiled_client[run_start_id]
            advice = give_advice(bluesky_run)
            publish__advice_documents(advice, advice_publisher)
        else:
            pass


    # this consumer should not be in a group with other consumers
    #   so generate a unique consumer group id for it
    unique_group_id = f"advise-{str(uuid.uuid4())[:8]}"

    kafka_dispatcher = RemoteDispatcher(
        topics=[f"cms.bluesky.runengine.documents"],
        bootstrap_servers=",".join(kafka_config["bootstrap_servers"]),
        group_id=unique_group_id,
        consumer_config=kafka_config["runengine_producer_config"],
    )

    kafka_dispatcher.subscribe(print_message)
    kafka_dispatcher.start()


if __name__ == "__main__":
    respond_to_stop_with_advice()
