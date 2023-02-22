import abc
from enum import Enum
from typing import TypeVar


class Step(Enum):
    pass


StepType = TypeVar("StepType", bound=Step)


class Stepper(abc.ABC):
    @abc.abstractmethod
    def update_step(self, new: StepType):
        """
        Update new step
        """

    @abc.abstractproperty
    def step(self):
        """
        Get now state
        """
