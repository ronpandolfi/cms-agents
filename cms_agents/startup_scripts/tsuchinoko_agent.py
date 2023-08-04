from bluesky_adaptive.server import register_variable, shutdown_decorator, startup_decorator

from cms_agents.tsuchinoko_agent import CMSTsuchinokoAgent

agent = CMSTsuchinokoAgent(report_on_tell=False, ask_on_tell=True)


@startup_decorator
def startup():
    agent.start()


@shutdown_decorator
def shutdown_agent():
    return agent.stop()


register_variable("tell cache", agent, "tell_cache")
register_variable("agent name", agent, "instance_name")
