# ponytail: shim for backwards compat — `preprocessing` renamed to `transform` in 0.8
from memframe.core.analytix.transform import TransformOps as PreprocessingOps
from memframe.core.analytix.transform import TransformOps

__all__ = ["PreprocessingOps", "TransformOps"]
