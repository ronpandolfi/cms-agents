import uuid
from typing import List, Sequence, Tuple, Union

import nslsii.kafka_utils
import numpy as np
import tiled
import torch
from bluesky_adaptive.agents.base import Agent, AgentConsumer
from bluesky_adaptive.agents.botorch import SingleTaskGPAgentBase
from bluesky_adaptive.agents.simple import SequentialAgentBase
from bluesky_kafka import Publisher
from bluesky_queueserver_api.zmq import REManagerAPI
from numpy.typing import ArrayLike


class CMSBaseAgent(Agent):
    """Base agent to interface with output of SciAnalysis stored in sandbox databroker"""

    def __init__(self, *args, **kwargs):
        self._independent_key = None
        self._target_key = None
        super().__init__(*args, **kwargs)

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
        return np.array(run.primary.data[self.independent_key]), np.array(run.primary.data[self.target_key])

    @property
    def independent_key(self):
        return self._independent_key

    @independent_key.setter
    def independent_key(self, value: str):
        self._independent_key = value

    @property
    def target_key(self):
        return self._target_key

    @target_key.setter
    def target_key(self, value: str):
        self._target_key = value

    def server_registrations(self) -> None:
        self._register_property("independent_key")
        self._register_property("target_key")
        return super().server_registrations()

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


class CMSSingleTaskAgent(CMSBaseAgent, SingleTaskGPAgentBase):
    def __init__(self, *, bounds: ArrayLike, **kwargs):
        """Single Task GP based Bayesian Optimization

        Parameters
        ----------
        bounds : ArrayLike
            A `2 x d` tensor of lower and upper bounds for each column of independent vars
        """
        _default_kwargs = self.get_beamline_objects()
        _default_kwargs.update(kwargs)
        bounds = torch.tensor(bounds)
        super().__init__(bounds=bounds, **_default_kwargs)
