# ponytail: shim — `preprocessing` → `transform`
from memframe.wrappers.analytix.transform import TransformWrapper as PreprocessingWrapper
from memframe.wrappers.analytix.transform import TransformWrapper, TransformAccessor

PreprocessAccessor = TransformAccessor

__all__ = ["PreprocessingWrapper", "TransformWrapper", "TransformAccessor", "PreprocessAccessor"]
