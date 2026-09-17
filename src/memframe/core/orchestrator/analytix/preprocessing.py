# ponytail: shim — `preprocessing` → `transform`
from memframe.core.orchestrator.analytix.transform import TransformOrchestrator as PreprocessingOrchestrator
from memframe.core.orchestrator.analytix.transform import TransformOrchestrator, TransformAccessor

PreprocessAccessor = TransformAccessor

__all__ = ["PreprocessingOrchestrator", "TransformOrchestrator", "TransformAccessor", "PreprocessAccessor"]
