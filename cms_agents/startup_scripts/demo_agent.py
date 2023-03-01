import threading

from bluesky_adaptive.server import register_variable, shutdown_decorator, start_task, startup_decorator

from cms_agents.agents import CMSSequentialAgent

dummy_object = None

agent = CMSSequentialAgent([0, 1, 2, 3, 4, 5])


@startup_decorator
def startup():
    # Start agent and its kafka consumer in background thread
    agent_thread = threading.Thread(target=agent.start, name="agent-loop", daemon=True)
    agent_thread.start()


@shutdown_decorator
def shutdown():
    return agent.stop()


def add_suggestions_to_queue(batch_size):
    start_task(agent.add_suggestions_to_queue, batch_size)


def generate_report(args_kwargs):
    """Cheap setter wrapper for generate report.
    All setters must take a single value, so this takes args and kwargs as a tuple to unpack.

    Parameters
    ----------
    args_kwargs : Tuple[List, dict]
        Tuple of args and kwargs passed to the API `POST` as a `value`
    """
    _, kwargs = args_kwargs
    start_task(agent.generate_report, **kwargs)


register_variable("add_suggestions_to_queue", setter=add_suggestions_to_queue)
register_variable("generate_report", setter=generate_report)
