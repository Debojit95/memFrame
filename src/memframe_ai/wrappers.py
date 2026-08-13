"""Bind public MemFrame wrappers to a Session so AI tools share one dispatch path.

`Session.wrappers` exposes one wrapper per domain bound to `session.ops`
(a `ContextManager`). AI tool files call `await w.<amethod>(...)` and
return through `normalize(...)` — the wrapper is the single dispatch path
for both interactive use and the agent fleet.
"""


class SessionWrappers:
    def __init__(self, session):
        from memframe.wrappers.analytix.cleaning import CleaningWrapper
        from memframe.wrappers.analytix.selection import SelectionWrapper
        from memframe.wrappers.analytix.inspection import TableOpsWrapper
        from memframe.wrappers.analytix.stats import StatsWrapper
        from memframe.wrappers.analytix.arithmetic import ArithmeticWrapper
        from memframe.wrappers.plots.bar import BarWrapper
        from memframe.wrappers.plots.line import LineWrapper
        from memframe.wrappers.plots.pie import PieWrapper
        from memframe.wrappers.plots.scatter import ScatterWrapper
        from memframe.wrappers.plots.scatter3d import Scatter3DWrapper
        from memframe.wrappers.plots.bar_polar import BarPolarWrapper

        self.cleaning = CleaningWrapper(session.ops)
        self.selection = SelectionWrapper(session.ops)
        self.inspection = TableOpsWrapper(session.ops)
        self.stats = StatsWrapper(session.ops)
        self.arithmetic = ArithmeticWrapper(session.ops)
        self.plot_bar = BarWrapper(session.ops)
        self.plot_line = LineWrapper(session.ops)
        self.plot_pie = PieWrapper(session.ops)
        self.plot_scatter = ScatterWrapper(session.ops)
        self.plot_scatter_3d = Scatter3DWrapper(session.ops)
        self.plot_bar_polar = BarPolarWrapper(session.ops)