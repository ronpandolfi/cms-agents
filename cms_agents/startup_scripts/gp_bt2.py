from typing import List, Tuple

from bluesky_adaptive.server import register_variable, shutdown_decorator, startup_decorator
from numpy.typing import ArrayLike

from cms_agents.agents import CMSSingleTaskAgent


class SingleTaskAgent(CMSSingleTaskAgent):
    def __init__(
        self,
        *,
        bounds: ArrayLike,
        independent_key: str = "metadata_extract__x_position",
        target_key: str = "value",
        **kwargs
    ):
        super().__init__(bounds=bounds, independent_key=independent_key, target_key=target_key, **kwargs)

    def measurement_plan(self, point: ArrayLike) -> Tuple[str, List, dict]:
        return "agent_feedback_plan", [point], dict()

    def trigger_condition(self, uid):
        return (
            self.independent_key in self.exp_catalog[uid].primary.data.keys()
            and self.target_key in self.exp_catalog[uid].primary.data.keys()
        )


agent = SingleTaskAgent(bounds=[0.0, 50.0], report_on_tell=False, ask_on_tell=False)


@startup_decorator
def startup():
    agent.start()


@shutdown_decorator
def shutdown_agent():
    return agent.stop()


register_variable("tell cache", agent, "tell_cache")
