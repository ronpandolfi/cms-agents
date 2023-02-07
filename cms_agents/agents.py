import uuid
from typing import Sequence, Tuple, Union

import nslsii.kafka_utils
from bluesky_adaptive.agents.simple import SequentialAgentBase
from numpy.typing import ArrayLike


class CMSBaseAgent:
    measurement_plan_name = ...

    @staticmethod
    def measurement_plan_args(point) -> list:
        """
        List of arguments to pass to plan from a point to measure.
        This is a good place to transform relative into absolute motor coords.
        """
        return ['pil2M']

    @staticmethod
    def measurement_plan_kwargs(point) -> dict:
        """
        Construct dictionary of keyword arguments to pass the plan, from a point to measure.
        This is a good place to transform relative into absolute motor coords.
        """
        return {}

    def unpack_run(self, run) -> Tuple[Union[float, ArrayLike], Union[float, ArrayLike]]:
        return 0, 0

    @staticmethod
    def get_beamline_kwargs() -> dict:
        beamline_tla = "cms"
        kafka_config = nslsii.kafka_utils._read_bluesky_kafka_config_file(
            config_file_path="/etc/bluesky/kafka.yml"
        )
        return dict(
            kafka_group_id=f"echo-{beamline_tla}-{str(uuid.uuid4())[:8]}",
            kafka_bootstrap_servers=','.join(kafka_config["bootstrap_servers"]),
            kafka_consumer_config=kafka_config["runengine_producer_config"],
            kafka_producer_config=kafka_config["runengine_producer_config"],
            publisher_topic=f"{beamline_tla}.bluesky.adjudicators",
            subscripion_topics=[
                f"{beamline_tla}.bluesky.runengine.documents",
            ],
            data_profile_name=f"{beamline_tla}",
            agent_profile_name=f"{beamline_tla}_bluesky_sandbox",
            qserver_host=f"https://qserver.nsls2.bnl.gov/{beamline_tla}",
            qserver_api_key=None,
        )


class CMSSequentialAgent(CMSBaseAgent, SequentialAgentBase):
    measurement_plan_name = 'count'

    def __init__(
        self,
        *,
        sequence: Sequence[Union[float, ArrayLike]],
        relative_bounds: Tuple[Union[float, ArrayLike]] = None,
        **kwargs,
    ) -> None:
        _default_kwargs = self.get_beamline_kwargs()
        _default_kwargs.update(kwargs)
        super().__init__(sequence=sequence, relative_bounds=relative_bounds, **_default_kwargs)
