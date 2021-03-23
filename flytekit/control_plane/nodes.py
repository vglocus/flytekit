import logging as _logging
from typing import Dict, List, Optional

from flytekit.common import constants as _constants
from flytekit.common import promise as _promise
from flytekit.common.core import identifier as _identifier
from flytekit.common.exceptions import scopes as _exception_scopes
from flytekit.common.exceptions import system as _system_exceptions
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.mixins import hash as _hash_mixin
from flytekit.common.nodes import ParameterMapper, OutputParameterMapper
from flytekit.common.utils import _dnsify
from flytekit.control_plane import component_nodes as _component_nodes
from flytekit.control_plane.tasks.task import FlyteTask
from flytekit.models import common as _common_models
from flytekit.models import task as _task_model
from flytekit.models.core import workflow as _workflow_model


class FlyteNode(_hash_mixin.HashOnReferenceMixin, _workflow_model.Node):
    def __init__(
        self,
        id,
        upstream_nodes,
        bindings,
        metadata,
        flyte_task: FlyteTask = None,
        flyte_workflow: "flytekit.control_plane.workflow.FlyteWorkflow" = None,
        flyte_launch_plan=None,
        flyte_branch=None,
        parameter_mapping=True,
    ):
        non_none_entities = list(filter(None, [flyte_task, flyte_workflow, flyte_launch_plan, flyte_branch]))
        if len(non_none_entities) != 1:
            raise _user_exceptions.FlyteAssertion(
                "An SDK node must have one underlying entity specified at once.  Received the following "
                "entities: {}".format(non_none_entities)
            )

        workflow_node = None
        if flyte_workflow is not None:
            workflow_node = _component_nodes.FlyteWorkflowNode(flyte_workflow=flyte_workflow)
        elif flyte_launch_plan is not None:
            workflow_node = _component_nodes.FlyteWorkflowNode(flyte_launch_plan=flyte_launch_plan)

        super(FlyteNode, self).__init__(
            id=_dnsify(id) if id else None,
            metadata=metadata,
            inputs=bindings,
            upstream_node_ids=[n.id for n in upstream_nodes],
            output_aliases=[],
            task_node=_component_nodes.FlyteTaskNode(flyte_task) if flyte_task else None,
            workflow_node=workflow_node,
            branch_node=flyte_branch,
        )
        self._upstream = upstream_nodes
        self._executable_flyte_object = flyte_task or flyte_workflow or flyte_launch_plan
        if parameter_mapping:
            if not flyte_branch:
                self._outputs = OutputParameterMapper(self._executable_flyte_object.interface.outputs, self)
            else:
                self._outputs = None

    @property
    def executable_flyte_object(self):
        return self._executable_flyte_object

    @classmethod
    def promote_from_model(
        cls,
        model: _workflow_model.Node,
        sub_workflows: Optional[Dict[_identifier.Identifier, _workflow_model.WorkflowTemplate]],
        tasks: Optional[Dict[_identifier.Identifier, _task_model.TaskTemplate]],
    ) -> "FlyteNode":
        id = model.id
        if id in {_constants.START_NODE_ID, _constants.END_NODE_ID}:
            _logging.warning(f"Should not call promote from model on a start node or end node {model}")
            return None

        flyte_task_node, flyte_workflow_node = None, None
        if model.task_node is not None:
            flyte_task_node = _component_nodes.FlyteTaskNode.promote_from_model(model.task_node, tasks)
        elif model.workflow_node is not None:
            flyte_workflow_node = _component_nodes.FlyteWorkflowNode.promote_from_model(
                model.workflow_node, sub_workflows, tasks
            )
        else:
            raise _system_exceptions.FlyteSystemException("Bad Node model, neither task nor workflow detected.")

        # When WorkflowTemplate models (containing node models) are returned by Admin, they've been compiled with a
        # start node. In order to make the promoted FlyteWorkflow look the same, we strip the start-node text back out.
        for model_input in model.inputs:
            if (
                model_input.binding.promise is not None
                and model_input.binding.promise.node_id == _constants.START_NODE_ID
            ):
                model_input.binding.promise._node_id = _constants.GLOBAL_INPUT_NODE_ID

        if flyte_task_node is not None:
            return cls(
                id=id,
                upstream_nodes=[],  # set downstream, model doesn't contain this information
                bindings=model.inputs,
                metadata=model.metadata,
                flyte_task=flyte_task_node.flyte_task,
            )
        elif flyte_workflow_node is not None:
            if flyte_workflow_node.flyte_workflow is not None:
                return cls(
                    id=id,
                    upstream_nodes=[],  # set downstream, model doesn't contain this information
                    bindings=model.inputs,
                    metadata=model.metadata,
                    flyte_workflow=flyte_workflow_node.flyte_workflow,
                )
            elif flyte_workflow_node.flyte_launch_plan is not None:
                return cls(
                    id=id,
                    upstream_nodes=[],  # set downstream, model doesn't contain this information
                    bindings=models.inputs,
                    metadata=model.metadata,
                    flyte_launch_plan=flyte_workflow_node.flyte_launch_plan,
                )
            raise _system_exceptions.FlyteSystemException(
                "Bad FlyteWorkflowNode model, both launch plan and workflow are None"
            )
        raise _system_exceptions.FlyteSystemException("Bad FlyteNode model, both task and workflow nodes are empty")

    @property
    def upstream_nodes(self) -> List["FlyteNode"]:
        return self._upstream

    @property
    def upstream_node_ids(self) -> List[str]:
        return list(sorted(n.id for n in self.upstream_nodes))

    @property
    def outputs(self) -> Dict[str, _promise.NodeOutput]:
        return self._outputs

    def assign_id_and_return(self, id: str):
        if self.id:
            raise _user_exceptions.FlyteAssertion(
                f"Error assigning ID: {id} because {self} is already assigned. Has this node been ssigned to another "
                "workflow already?"
            )
        self._id = _dnsify(id) if id else None
        self._metadata.name = id
        return self

    def with_overrides(self, *args, **kwargs):
        # TODO: Implement overrides
        raise NotImplementedError("Overrides are not supported in Flyte yet.")

    @_exception_scopes.system_entry_point
    def __lshift__(self, other: "FlyteNode") -> "FlyteNode":
        """
        Add a node upstream of this node without necessarily mapping outputs -> inputs.
        :param other: node to place upstream
        """
        if hash(other) not in set(hash(n) for n in self.upstream_nodes):
            self._upstream.append(other)
        return other

    @_exception_scopes.system_entry_point
    def __rshift__(self, other: "FlyteNode") -> "FlyteNode":
        """
        Add a node downstream of this node without necessarily mapping outputs -> inputs.

        :param other: node to place downstream
        """
        if hash(self) not in set(hash(n) for n in other.upstream_nodes):
            other.upstream_nodes.append(self)
        return other

    def __repr__(self) -> str:
        return f"Node(ID: {self.id} Executable: {self._executable_flyte_object})"
