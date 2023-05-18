from typing import Dict, Sequence, Tuple, Union

import numpy as np
from gpcam.gp_optimizer import GPOptimizer
from numpy.typing import ArrayLike
from scipy.optimize import NonlinearConstraint

from cms_agents.agents import CMSBaseAgent

initial_ask_rng = np.random.default_rng(20230518)

## setting up parameters
# motor speed in x, x here is the "other" dimension (in 2d) that we can freely move through
v = 0.1  # in unit [x per second]

# bounds x,t
end_of_time = 100  # [seconds]
bounds = np.array([[-1, 1], [0, 2.0 * end_of_time]])
print("bounds")
print(bounds)
time_buffer = 5.0  # [seconds] We need a time buffer to account for optimization time of the acquisition function

init_N = 10  # don't go too low here

# create an initial array
# jlynch: this has been implemented inside the agent class ask method
# x_init = np.random.uniform(low = bounds[:,0], high = np.array([1.,5.]), size = (init_N,2))
# print(x_init)
# why is end_of_time and bounds[1,1] not the same? Easy, the optimizer does not like to be pushed into
# a corner when the the volume between the constraint and the bound gets too small. Instability results.

hps_bounds = np.array(
    [[0.0001, 10.0], [0.01, 10.0], [0.01, 10.0 * end_of_time]]
)  ##set up the hyperparameter bounds

## this is where the communication with the instrument is defined, the acquisition function,
## the kernel and all other GP-related things


# we may set up a user-defined acquisition function, but we can also use a standard one provided by gpCAM
def acq_func(x, obj):
    #    a = 3.0 #3.0 for 95 percent confidence interval
    #    mean = obj.posterior_mean(x)["f(x)"]
    cov = obj.posterior_covariance(x)["v(x)"]
    return np.sqrt(cov)


# Constrained optimization sometimes fails and so, as a safety net, we assign really high costs to past measurements.
# Otherwise costs just rise with how long we have to wait for the measurement to occur
def cost(origin, x, arguments=None):
    if origin[1] > x[0, 1]:
        return 100000.0
    time = np.abs(x[0, 1] - origin[1])
    return time


class CMSgpCAMAgent(CMSBaseAgent):
    """
    These agents should respond to messages with topic "cms.bluesky.reduced.documents".
    """

    def __init__(self, **kwargs):
        _default_kwargs = self.get_beamline_objects()
        _default_kwargs.update(**kwargs)
        super().__init__(independent_key=None, target_key=None, **_default_kwargs)

        self.independent_cache = list()
        self.observable_cache = list()

        self.bounds = bounds
        self.gp_optimizer = GPOptimizer(2, bounds)
        self.gp_optimizer_initialized = False

    # the parent class trigger_condition() always returns True
    # this agent should respond to every run, so this is what we want

    # use CMSAgent.measurement_plan() for now
    # def measurement_plan(self, point) -> Tuple[str, List, dict]:
    #     return super().measurement_plan(point)

    def unpack_run(self, run) -> Tuple[Union[float, ArrayLike], Union[float, ArrayLike]]:
        """Unpack information from the "reduction" step

        The event document data includes keys "value" and "variance",
        which are intended for gpCAM.
        """
        # base class unpack_run won't work
        # return super().unpack_run(run)

        # this is from the previous beamtime
        sample_theta = run.metadata["start"]["raw_start"]["sample_th"]
        sample_x = run.metadata["start"]["raw_start"]["sample_x"]

        value = run.primary.data["value"].read()

        return (np.array([sample_theta, sample_x]), value)

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
        self.independent_cache.append(x)
        self.observable_cache.append(y)

        x_data = np.array(self.independent_cache)
        y_data = np.array(self.observable_cache)

        if not self.gp_optimizer_initialized:
            self.gp_optimizer.tell(x=x_data, y=y_data)
            if len(self.independent_cache) >= 10:
                self.gp_optimizer.init_gp(np.ones(3))
                self.gp_optimizer.train_gp(hps_bounds)
                self.gp_optimizer.init_cost(cost, dict())

                self.gp_optimizer_initialized = True

                def g(x):
                    current_time = self.gp_optimizer.x_data[-1, 1]
                    current_x = self.gp_optimizer.x_data[-1, 0]
                    if ((x[1] - (current_time + time_buffer)) * v) ** 2 - (x[0] - current_x) ** 2 < 0.0:
                        return -1.0
                    elif x[1] - (current_time + time_buffer) < 0.0:
                        return -1.0
                    else:
                        return np.sqrt(((x[1] - (current_time + time_buffer)) * v) ** 2 - (x[0] - current_x) ** 2)

                self.nlc = NonlinearConstraint(g, 0, np.inf)

        else:
            self.auto_experimenter.gp_optimizer.tell(x=x_data, y=y_data, variances=(0.1 * np.ones_like(y.shape)))
            # don't forget about the initial data
            if len(self.independent_cache) in [15, 20, 25]:
                print("training gp_optimizer")
                self.gp_optimizer.train_gp(hps_bounds)

        return dict(independent_variable=x, observable=y)

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

        if not self.gp_optimizer_initialized:
            ask_result = dict(
                x=initial_ask_rng.uniform(low=self.bounds[:, 0], high=[1, time_buffer], size=(batch_size, 2))
            )
            # we want the last "position" before initializing the gp_optimizer
            self.current_position = self.independent_cache[-1]
        else:
            ask_result = self.auto_experimenter.gp_optimizer.ask(
                position=self.current_position,
                n=batch_size,
                acquisition_function="shannon_ig",  ##you can use your own acqisition function here
                bounds=None,
                pop_size=20,
                max_iter=20,
                tol=1e-6,
                constraints=(self.nlc,),
                vectorized=False,
            )

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
