import os
import pathlib
import time

import pytest

from flytekit.control_plane import launch_plan, workflow
from flytekit.models.core.identifier import Identifier, ResourceType
from flytekit.models import literals

PROJECT = "flytesnacks"
VERSION = os.getpid()


@pytest.fixture(scope="session")
def flyte_workflows_source_dir():
    return pathlib.Path(os.path.dirname(__file__)) / "mock_flyte_repo"


@pytest.fixture(scope="session")
def flyte_workflows_register(docker_compose):
    docker_compose.execute(
        f"exec -w /flyteorg/src -e SANDBOX=1 -e PROJECT={PROJECT} -e VERSION=v{VERSION} "
        "backend make -C workflows register"
    )


def test_client(flyteclient):
    projects = flyteclient.list_projects_paginated(limit=5, token=None)
    assert len(projects) <= 5


def test_launch_workflow(flyteclient, flyte_workflows_register):
    for wf, data in [
        ("workflows.basic.hello_world.my_wf", literals.LiteralMap({})),
    ]:
        lp = launch_plan.FlyteLaunchPlan.fetch(PROJECT, "development", wf, f"v{VERSION}")
        execution = lp.launch_with_literals(PROJECT, "development", data)
        execution.wait_for_completion()
        print(execution.id)


def test_get_workflow(flyteclient, flyte_workflows_register):
    wf_id = Identifier(
        ResourceType.WORKFLOW, PROJECT, "development", "workflows.basic.basic_workflow.my_wf", f"v{VERSION}"
    )
    wf = flyteclient.get_workflow(wf_id)
    print(wf)
    assert wf_id == wf.id


def test_launch_workflow_with_args(flyteclient, flyte_workflows_register):
    for wf, data in [
        (
            "workflows.basic.basic_workflow.my_wf",
            literals.LiteralMap(
                {
                    "a": literals.Literal(literals.Scalar(literals.Primitive(integer=10))),
                    "b": literals.Literal(literals.Scalar(literals.Primitive(string_value="foobar"))),
                }
            ),
        )
    ]:
        lp = launch_plan.FlyteLaunchPlan.fetch(PROJECT, "development", wf, f"v{VERSION}")
        execution = lp.launch_with_literals(PROJECT, "development", data)
        execution.wait_for_completion()
        assert execution.outputs.literals["o0"].scalar.primitive.integer == 12
        assert execution.outputs.literals["o1"].scalar.primitive.string_value == "foobarworld"
