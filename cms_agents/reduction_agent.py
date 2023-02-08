import datetime
import pprint
import uuid

from bluesky_kafka import Publisher, RemoteDispatcher
import nslsii.kafka_utils

from tiled.client import from_profile


def reduce_run(bluesky_run):
    print(f"give reduced on run {bluesky_run}")
    reduced = {
        "next big thing": "avocado toast"
    }

    return reduced


def publish_reduced_documents(reduced, reduced_publisher):



def respond_to_stop_with_reduced():

    kafka_config = nslsii.kafka_utils._read_bluesky_kafka_config_file(config_file_path="/etc/bluesky/kafka.yml")

    cms_tiled_client = from_profile("cms")

    reduced_publisher = Publisher(
        topic="",
        bootstrap_servers=",".join(kafka_config["bootstrap_servers"]),
        producer_config=kafka_config["producer_config"]
    )

    def on_stop_reduce_run(name, doc):
        print(
            f"{datetime.datetime.now().isoformat()} document: {name}\n"
            f"contents: {pprint.pformat(doc)}\n"
        )
        if name == "stop":
            # look up the results of this run
            run_start_id = doc["run_start"]
            print(f"found run_start id {run_start_id}")
            bluesky_run = cms_tiled_client[run_start_id]
            reduced = reduce_run(bluesky_run)
            publish_documents(reduced, reduced_publisher)
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
    respond_to_stop_with_reduced()
