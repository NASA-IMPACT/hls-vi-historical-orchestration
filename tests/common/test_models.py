from common.models import GranuleProcessingEvent


class TestGranuleProcessingEvent:
    """Test GranuleProcessingEvent"""

    def to_from_envvar(self):
        event = GranuleProcessingEvent(
            granule_id="foo",
            attempt=42,
        )
        env = event.to_envvar()
        event_from_envvar = GranuleProcessingEvent.from_envvar(env)

        assert event == event_from_envvar
