from typing import Dict, List

from flytekit.clients.helpers import iterate_node_executions as _iterate_node_executions
from flytekit.common.mixins import artifact as _artifact
from flytekit.control_plane import identifier as _core_identifier
from flytekit.control_plane import nodes as _nodes
from flytekit.models import execution as _execution_models
from flytekit.models import filters as _filter_models


class FlyteWorkflowExecution(_execution_models.Execution, _artifact.ExecutionArtifact):
    def __init__(self, *args, **kwargs):
        super(FlyteWorkflowExecution, self).__init__(*args, **kwargs)
        self._node_executions = None
        self._inputs = None
        self._outputs = None

    @property
    def node_executions(self) -> Dict[str, _nodes.FlyteNodeExecution]:
        return self._node_executions or {}

    @property
    def inputs(self):
        # TODO
        pass

    @property
    def outputs(self):
        # TODO
        pass

    @property
    def error(self) -> _execution_models.ExecutionError:
        """
        If execution is in progress, raise an exception.  Otherwise, return None if no error was present upon
        reaching completion.
        """
        if not self.is_complete:
            raise _user_exceptions.FlyteAssertion(
                "Please wait until a workflow has completed before checking for an " "error."
            )
        return self.closure.error

    @property
    def is_complete(self) -> bool:
        """
        Whether or not the execution is complete.
        """
        return self.closure.phase in {
            _core_execution_models.WorkflowExecutionPhase.ABORTED,
            _core_execution_models.WorkflowExecutionPhase.FAILED,
            _core_execution_models.WorkflowExecutionPhase.SUCCEEDED,
            _core_execution_models.WorkflowExecutionPhase.TIMED_OUT,
        }

    @classmethod
    def promote_from_model(cls, base_model: _execution_models.Execution) -> "FlyteWorkflowExecution":
        return cls(
            closure=base_model.closure,
            id=_core_identifier.WorkflowExecutionIdentifier.promote_from_model(base_model.id),
            spec=base_model.spec,
        )

    @classmethod
    def fetch(cls, project: str, domain: str, name: str) -> "FlyteWorkflowExecution":
        return cls.promote_from_model(
            _flyte_engine.get_client().get_execution(
                _core_identifier.WorkflowExecutionIdentifier(project=project, domain=domain, name=name)
            )
        )

    def sync(self):
        """
        Syncs the state of the underlying execution artifact with the state observed by the platform.
        """
        if not self.is_complete or self._node_executions is None:
            self._sync_closure()
            self._node_executions = self.get_node_executions()

    def _sync_closure(self):
        if not self.is_complete:
            client = _flyte_engine.get_client()
            self._closure = client.get_execution(self.id).closure

    def get_node_executions(self, filters: List[_filter_models.Filter] = None) -> Dict[str, _nodes.FlyteNodeExecution]:
        client = _flyte_engine.get_client()
        return {
            node.id.node_id: _nodes.FlyteNodeExecution.promote_from_model(node)
            for node in _iterate_node_executions(client, self.id, filters=filters)
        }

    def terminate(self, cause: str):
        _flyte_engine.get_client().terminate_execution(self.id, cause)
