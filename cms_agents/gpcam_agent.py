from cms_agents.agents import CMSBaseAgent 


class CMSgpCAMAgent(CMSBaseAgent):
    def __init__(self):
        _default_kwargs = self.get_beamline_objects()
        super().__init__(**_default_kwargs)

    def ask():
        pass

    def tell():
        pass