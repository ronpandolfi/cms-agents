import uuid
from typing import List, Sequence, Tuple, Union

import nslsii.kafka_utils
import tiled
from bluesky_adaptive.agents.base import AgentConsumer
from bluesky_adaptive.agents.simple import SequentialAgentBase
from bluesky_kafka import Publisher
from bluesky_queueserver_api.zmq import REManagerAPI
from numpy.typing import ArrayLike


class CMSBaseAgent:
    def measurement_plan(self, point: ArrayLike) -> Tuple[str, List, dict]:
        """Default measurement plan is a count on the pilatus, for a given num

        Parameters
        ----------
        point : ArrayLike
            Next point to measure using a given plan

        Returns
        -------
        plan_name : str
        plan_args : List
            List of arguments to pass to plan from a point to measure.
        plan_kwargs : dict
            Dictionary of keyword arguments to pass the plan, from a point to measure.
        """
        return "count", [["pilatus2M"]], dict(num=point)

    def unpack_run(self, run) -> Tuple[Union[float, ArrayLike], Union[float, ArrayLike]]:
        print(run)
        return 0, 0

    @staticmethod
    def get_beamline_objects() -> dict:
        beamline_tla = "cms"
        kafka_config = nslsii.kafka_utils._read_bluesky_kafka_config_file(
            config_file_path="/etc/bluesky/kafka.yml"
        )
        qs = REManagerAPI(zmq_control_addr="tcp://xf11bm-ws1.nsls2.bnl.local:60615")

        kafka_consumer = AgentConsumer(
            topics=[
                f"{beamline_tla}.bluesky.reduced.documents",
            ],
            consumer_config=kafka_config["runengine_producer_config"],
            bootstrap_servers=kafka_config["bootstrap_servers"],
            group_id=f"echo-{beamline_tla}-{str(uuid.uuid4())[:8]}",
        )

        kafka_producer = Publisher(
            topic=f"{beamline_tla}.bluesky.adjudicators",
            bootstrap_servers=kafka_config["bootstrap_servers"],
            key="cms.key",
            producer_config=kafka_config["runengine_producer_config"],
        )

        return dict(
            kafka_consumer=kafka_consumer,
            kafka_producer=kafka_producer,
            tiled_data_node=tiled.client.from_profile(f"{beamline_tla}_bluesky_sandbox"),
            tiled_agent_node=tiled.client.from_profile(f"{beamline_tla}_bluesky_sandbox"),
            qserver=qs,
        )

    @staticmethod
    def get_beamline_kwargs() -> dict:
        beamline_tla = "cms"
        kafka_config = nslsii.kafka_utils._read_bluesky_kafka_config_file(
            config_file_path="/etc/bluesky/kafka.yml"
        )
        qs = REManagerAPI(zmq_control_addr="tcp://xf11bm-ws1.nsls2.bnl.local:60615")

        return dict(
            kafka_group_id=f"echo-{beamline_tla}-{str(uuid.uuid4())[:8]}",
            kafka_bootstrap_servers=",".join(kafka_config["bootstrap_servers"]),
            kafka_consumer_config=kafka_config["runengine_producer_config"],
            kafka_producer_config=kafka_config["runengine_producer_config"],
            publisher_topic=f"{beamline_tla}.bluesky.adjudicators",
            subscripion_topics=[
                f"{beamline_tla}.bluesky.reduced.documents",
            ],
            data_profile_name=f"{beamline_tla}_bluesky_sandbox",
            agent_profile_name=f"{beamline_tla}_bluesky_sandbox",
            qserver=qs,
        )


class CMSSequentialAgent(CMSBaseAgent, SequentialAgentBase):
    def __init__(
        self,
        *,
        sequence: Sequence[Union[float, ArrayLike]],
        relative_bounds: Tuple[Union[float, ArrayLike]] = None,
        **kwargs,
    ) -> None:
        _default_kwargs = self.get_beamline_objects()
        _default_kwargs.update(kwargs)
        super().__init__(sequence=sequence, relative_bounds=relative_bounds, **_default_kwargs)
