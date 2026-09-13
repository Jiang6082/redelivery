from dataclasses import dataclass

import pytest

from redelivery import Inbox


@dataclass
class Clock:
    now: float = 1000.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture
def inbox(tmp_path, clock):
    return Inbox(tmp_path / "inbox.db", clock=clock)
