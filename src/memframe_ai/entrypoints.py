from memframe.utils.async_sync import async_to_sync

from memframe_ai.agents import agent_for
from memframe_ai.config import AISettings
from memframe_ai.observe import logger
from memframe_ai.sessions import store


def _default_session_id(ops) -> str:
    data_id = ops._data_id or ops.memframe._active_id or "anon"
    return f"{id(ops.memframe)}:{data_id}"


def _get_settings(memframe) -> AISettings:
    settings = getattr(memframe, "_ai_settings", None)
    if settings is None:
        raise RuntimeError(
            "No AI agent configured. Call mf.enable_agent(...) or await mf.aenable_agent(...) first."
        )
    return settings


async def achat(self, prompt: str, session_id: str | None = None, return_blocks: bool = False) -> dict:
    settings = _get_settings(self.memframe)
    sid = session_id or _default_session_id(self)
    logger.info("achat session=%s user_query='%s' return_blocks=%s", sid, prompt, return_blocks)
    session = store.get(sid)
    if session is None:
        session = store.create(sid, ops=self, settings=settings)
    return await agent_for(session).achat(prompt, return_blocks=return_blocks)


chat = async_to_sync(achat)


async def aenable_agent(
    self,
    api_key: str,
    provider: str | None = None,
    model: str | None = None,
    **overrides,
) -> AISettings:
    kwargs = {"api_key": api_key}
    if provider is not None:
        kwargs["provider"] = provider
    if model is not None:
        kwargs["model"] = model
    kwargs.update(overrides)
    settings = AISettings(**kwargs)
    self._ai_settings = settings
    return settings


enable_agent = async_to_sync(aenable_agent)
