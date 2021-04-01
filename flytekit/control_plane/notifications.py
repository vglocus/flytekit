from typing import List

from flyteidl.admin import common_pb2 as _common_pb2

from flytekit.models import common as _common_model
from flytekit.models import execution as _execution_model
from flytekit.models.core import execution as _core_execution_model


Phases = List[_core_execution_model.WorkflowExecutionPhase]


class Notification(_common_model.Notification):

    VALID_PHASES = {
        _execution_model.WorkflowExecutionPhase.ABORTED,
        _execution_model.WorkflowExecutionPhase.FAILED,
        _execution_model.WorkflowExecutionPhase.SUCCEEDED,
        _execution_model.WorkflowExecutionPhase.TIMED_OUT,
    }

    def __init__(self, phases: Phases, email=None, pager_duty=None, slack=None):
        """
        :param list[int] phases: A required list of phases for which to fire the event.  Events can only be fired for
            terminal phases.  Phases should be as defined in: flytekit.models.core.execution.WorkflowExecutionPhase
        """
        self._validate_phases(phases)
        super(Notification, self).__init__(phases, email=email, pager_duty=pager_duty, slack=slack)

    def _validate_phases(self, phases: Phases):
        """
        :param phases:
        """
        if len(phases) == 0:
            raise _user_exceptions.FlyteAssertion("You must specify at least one phase for a notification.")
        for phase in phases:
            if phase not in self.VALID_PHASES:
                raise _user_exceptions.FlyteValueException(
                    phase,
                    self.VALID_PHASES,
                    additional_message="Notifications can only be specified on terminal states.",
                )

    @classmethod
    def from_flyte_idl(cls, p: _common_pb2.Notification) -> "Notification":
        """
        :param p: FlyteIDL Notification
        """
        if p.HasField("email"):
            return cls(p.phases, p.email.recipients_email)
        elif p.HasField("pager_duty"):
            return cls(p.phases, p.pager_duty.recipients_email)
        else:
            return cls(p.phases, p.slack.recipients_email)


class PagerDuty(Notification):
    def __init__(self, phases: Phases, recipients_email: List[str]):
        """
        :param phases: A required list of phases for which to fire the event.  Events can only be fired for terminal
            phases.
        :param recipients_email: A required non-empty list of recipients for the notification.
        """
        super(PagerDuty, self).__init__(phases, pager_duty=_common_model.PagerDutyNotification(recipients_email))

    @classmethod
    def promote_from_model(cls, base_model: _common_model.Notification) -> Notification:
        return cls(base_model.phases, base_model.pager_duty.recipients_email)


class Email(Notification):
    def __init__(self, phases: Phases, recipients_email: List[str]):
        """
        :param phases: A required list of phases for which to fire the event.  Events can only be fired for terminal
            phases.
        :param recipients_email: A required non-empty list of recipients for the notification.
        """
        super(Email, self).__init__(phases, email=_common_model.EmailNotification(recipients_email))

    @classmethod
    def promote_from_model(cls, base_model: _common_model.Notification) -> "Notification":
        """
        :param base_model:
        """
        return cls(base_model.phases, base_model.email.recipients_email)


class Slack(Notification):
    def __init__(self, phases: Phases, recipients_email: List[str]):
        """
        :param phases: A required list of phases for which to fire the event.  Events can only be fired for terminal
            phases.
        :param recipients_email: A required non-empty list of recipients for the notification.
        """
        super(Slack, self).__init__(phases, slack=_common_model.SlackNotification(recipients_email))

    @classmethod
    def promote_from_model(cls, base_model: _common_model.Notification) -> "Notification":
        """
        :param base_model:
        """
        return cls(base_model.phases, base_model.slack.recipients_email)
