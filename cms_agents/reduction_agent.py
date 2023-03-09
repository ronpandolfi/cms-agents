import argparse
import datetime
import pprint
import uuid
import time as ttime

from bluesky_kafka import Publisher, RemoteDispatcher
import nslsii.kafka_utils

from ophyd.utils.epics_pvs import data_shape, data_type
from event_model import compose_run

from tiled.client import from_profile


def reduce_run(bluesky_run):
    """
    Reduce data from a single bluesky run.

    Parameters
    ----------
    bluesky_run : BlueskyRun
        The run to be reduced, assumed to be a v2 run object.

    Returns
    -------
    reduced : dict
        A single event

    metadata : dict
        Will be top-level in the reduced start document
    """
    print(f"give reduced on run {bluesky_run}")
    reduced = {"next big thing": "avocado toast"}

    return reduced, {"raw_start": bluesky_run.metadata["start"]}


def publish_reduced_documents(reduced, metadata, reduced_publisher):
    cr = compose_run(metadata=metadata)
    reduced_publisher("start", cr.start_doc)

    desc_bundle = cr.compose_descriptor(
        name="primary",
        data_keys={
            k: {
                "dtype": data_type(v),
                "shape": data_shape(v),
                "source": "computed",
            }
            for k, v in reduced.items()
        },
    )

    reduced_publisher("descriptor", desc_bundle.descriptor_doc)
    t = ttime.time()
    reduced_publisher(
        "event",
        desc_bundle.compose_event(
            data=reduced,
            timestamps={k: t for k in reduced},
        ),
    )

    reduced_publisher("stop", cr.compose_stop())


def respond_to_stop_with_reduced(consumer_topic, testing):

    kafka_config = nslsii.kafka_utils._read_bluesky_kafka_config_file(config_file_path="/etc/bluesky/kafka.yml")

    cms_tiled_client = from_profile("cms")
    
    if testing:
        def do_both(name, doc):
            print(
                f"{datetime.datetime.now().isoformat()} output document: {name}\n"
                f"contents: {pprint.pformat(doc)}\n"
            )
    else:
        cms_sandbox_tiled_client = from_profile("cms_bluesky_sandbox")
        reduced_publisher = Publisher(
            key="",
            topic="cms.bluesky.reduced.documents",
            bootstrap_servers=",".join(kafka_config["bootstrap_servers"]),
            producer_config=kafka_config["runengine_producer_config"],
        )

        def do_both(name, doc):
            cms_sandbox_tiled_client.v1.insert(name, doc)
            reduced_publisher(name, doc)

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
            reduced, metadata = reduce_run(bluesky_run)
            publish_reduced_documents(reduced, metadata, do_both)
        else:
            pass

    # this consumer should not be in a group with other consumers
    #   so generate a unique consumer group id for it
    unique_group_id = f"reduce-{str(uuid.uuid4())[:8]}"

    kafka_dispatcher = RemoteDispatcher(
        topics=[consumer_topic],
        bootstrap_servers=",".join(kafka_config["bootstrap_servers"]),
        group_id=unique_group_id,
        consumer_config=kafka_config["runengine_producer_config"],
    )

    kafka_dispatcher.subscribe(on_stop_reduce_run)
    kafka_dispatcher.start()


def get_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--consumer-topic",
        default="cms.bluesky.runengine.documents",
        help="Kafka topic for reduction_agent input",
    )

    parser.add_argument(
        "--testing",
        default=False,
        action="store_true",
        help="reduction_agent will not generate output"
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = get_args()
    respond_to_stop_with_reduced(**vars(args))
