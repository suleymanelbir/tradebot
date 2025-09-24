class PositionSupervisor:
    def __init__(self, cfg, portfolio, notifier, persistence):
        self.cfg = cfg; self.portfolio = portfolio
        self.notifier = notifier; self.persistence = persistence
