from typing import List, Sequence, Tuple, Union

from bluesky_adaptive.server import shutdown_decorator, startup_decorator
from numpy.typing import ArrayLike

from cms_agents.agents import CMSSequentialAgent


class SequentialAgent(CMSSequentialAgent):
    def __init__(
        self,
        *args,
        sequence: Sequence[Union[float, ArrayLike]],
        independent_key: str = "metadata_extract__x_position",
        target_key: str = "value",
        relative_bounds: Tuple[Union[float, ArrayLike]] = None,
        **kwargs
    ):
        super().__init__(
            *args,
            independent_key=independent_key,
            target_key=target_key,
            sequence=sequence,
            relative_bounds=relative_bounds,
            **kwargs
        )

    def measurement_plan(self, point: ArrayLike) -> Tuple[str, List, dict]:
        return "agent_feeback_plan", [point], dict()


agent = SequentialAgent(sequence=[0.0, 5.0, 10.0, 15.0, 20.0])


@startup_decorator
def startup():
    agent.start()


@shutdown_decorator
def shutdown_agent():
    return agent.stop()
