from typing import Dict, Sequence, Tuple, Union

import numpy as np
from gpcam.autonomous_experimenter import AutonomousExperimenterGP
from numpy.typing import ArrayLike

from cms_agents.agents import CMSBaseAgent


# taken from basic gpCAM example
#   https://gpcam.lbl.gov/examples/minimal-gpcam7-example
def instrument(data):
    for entry in data:
        entry["value"] = np.sin(np.linalg.norm(entry["position"]))
    return data


parameter_bounds = np.array([[3.0, 45.8], [4.0, 47.0]])
init_hyperparameters = np.array([1, 1, 1])
hyperparameter_bounds = np.array([[0.01, 100], [0.01, 100.0], [0.01, 100]])

auto_experimenter = AutonomousExperimenterGP(
    parameter_bounds=parameter_bounds,
    hyperparameters=init_hyperparameters,
    hyperparameter_bounds=hyperparameter_bounds,
    instrument_func=instrument,
    init_dataset_size=10,
)


class CMSgpCAMAgent(CMSBaseAgent):
    """
    These agents should respond to messages with topic "cms.bluesky.reduced.documents".
    """

    def __init__(self):
        _default_kwargs = self.get_beamline_objects()
        super().__init__(independent_key=None, target_key="value", **_default_kwargs)
        self.auto_experimenter = auto_experimenter

    # use CMSAgent.measurement_plan() for now
    # def measurement_plan(self, point) -> Tuple[str, List, dict]:
    #     return super().measurement_plan(point)

    def unpack_run(self, run) -> Tuple[Union[float, ArrayLike], Union[float, ArrayLike]]:
        """Unpack information from the "reduction" step

        The event document data includes keys "value" and "variance",
        which are intended for gpCAM.
        """
        # base class unpack_run probably won't work
        return super().unpack_run(run)

    def tell(self, x, y) -> Dict[str, ArrayLike]:
        """
        Parameters
        ----------
          x : ArrayLike
          y : ArrayLike

        example:
        In [2]: my_ae.gp_optimizer.tell(
           ...:     x=np.array([[12.07704017, 21.76965657]]),
           ...:     y=np.array([-2.])
           ...: )


        """
        self.auto_experimenter.gp_optimizer.tell(x=x, y=y)
        return dict(independent_variable=x, observable=y, cache_len=len(self.independent_cache))

    def ask(self, batch_size) -> Tuple[Sequence[Dict[str, ArrayLike]], Sequence[ArrayLike]]:
        """
        TODO: provide additional parameters to gp_optimizer.ask()
          such as acquisition_function, method, etc
          maybe provided to __init__
        example of gp_optimizer.ask() return value:
          In [4]: my_ae.gp_optimizer.ask(n=3)
          Out[4]:
            {
              'x': array(
                [
                  [31.65024711, 20.74185173],
                  [13.44485402, 43.78775007],
                  [20.64840344, 11.97894979]
                 ]
              ),
              'f(x)': array([-4.2568156]),
              'opt_obj': None
            }
        """

        ask_result = self.auto_experimenter.gp_optimizer.ask(n=batch_size)
        return (
            [
                dict(
                    suggestion=suggested_x,
                    latest_data=self.tell_cache[-1],
                    cache_len=self.inputs.shape[0],
                )
                for suggested_x in ask_result["x"]
            ],
            # this must be a sequence so unpack rows of
            # ask_result["x"] into a list
            [suggested_x for suggested_x in ask_result["x"]],
        )
