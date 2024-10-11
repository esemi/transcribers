from abc import ABC, abstractmethod


class BaseTranscribator(ABC):
    _language: str

    def __init__(self, language: str) -> None:
        self._language = language

    @abstractmethod
    def transcribe(self, audio_path: str, speaker_labeling: bool = False) -> list[str]:
        pass
