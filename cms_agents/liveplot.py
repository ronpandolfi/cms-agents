
import matplotlib.pyplot as plt

from bluesky.callbacks.best_effort import BestEffortCallback
from bluesky_kafka import RemoteDispatcher
from nslsii.kafka_utils import _read_bluesky_kafka_config_file

plt.rcParams['figure.raise_window'] = False

bec = BestEffortCallback()


def parse_bluesky_kafka_config_file(config_file_path):
    raw_bluesky_kafka_config = _read_bluesky_kafka_config_file(config_file_path=config_file_path)

    # convert the list of bootstrap servers into a comma-delimited string
    #   this is the format required by the confluent python api
    bootstrap_servers = ",".join(raw_bluesky_kafka_config["bootstrap_servers"])

    # extract security configuration
    #   it might be a good idea to explicitly specify consumer configuration(s)
    #   in /etc/bluesky/kafka.yml
    security_config = {
        k: raw_bluesky_kafka_config["runengine_producer_config"][k]
        for k in ("security.protocol", "sasl.mechanisms", "sasl.username", "sasl.password", "ssl.ca.location")
    }

    return bootstrap_servers, security_config


bootstrap_servers, security_config = parse_bluesky_kafka_config_file('/etc/bluesky/kafka.yml')

document_to_workflow_dispatcher = RemoteDispatcher(
        topics=["cms.bluesky.runengine.documents"],
        bootstrap_servers=bootstrap_servers,
        group_id="cms-liveplot",
        consumer_config={"auto.offset.reset": "latest", **security_config},
    )
document_to_workflow_dispatcher.subscribe(bec)
document_to_workflow_dispatcher.start(work_during_wait=lambda: plt.pause(0.05))