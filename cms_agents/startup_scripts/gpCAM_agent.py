from bluesky_adaptive.server import register_variable, shutdown_decorator, startup_decorator

from cms_agents.gpcam_agent import CMSgpCAMAgent

agent = CMSgpCAMAgent(report_on_tell=False, ask_on_tell=False)


@startup_decorator
def startup():
    agent.start()


@shutdown_decorator
def shutdown_agent():
    return agent.stop()


register_variable("tell cache", agent, "tell_cache")
register_variable("agent name", agent, "instance_name")
