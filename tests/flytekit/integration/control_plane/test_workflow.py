import os
import pathlib
import time

import pytest

from flytekit.control_plane import launch_plan
from flytekit.models import literals

PROJECT = "flytesnacks"


@pytest.fixture(scope="session")
def flyte_workflows_source_dir():
    return pathlib.Path(os.path.dirname(__file__)) / "mock_flyte_repo"


@pytest.fixture(scope="session")
def flyte_workflows_register(docker_compose):
    docker_compose.execute(
        f"exec -w /flyteorg/src -e SANDBOX=1 -e PROJECT={PROJECT} -e VERSION=v{os.getpid()} "
        "backend make -C workflows register"
    )


def test_client(flyteclient):
    projects = flyteclient.list_projects_paginated(limit=5, token=None)
    assert len(projects) <= 5


def test_launch_workflow(flyteclient, flyte_workflows_register):
    execution = launch_plan.FlyteLaunchPlan.fetch(
        PROJECT, "development", "workflows.basic.basic_workflow.my_wf", f"v{os.getpid()}"
    ).launch_with_literals(
        PROJECT,
        "development",
        literals.LiteralMap(
            {
                "a": literals.Literal(literals.Scalar(literals.Primitive(integer=10))),
                "b": literals.Literal(literals.Scalar(literals.Primitive(string_value="foobar"))),
            }
        ),
    )

    for _ in range(20):
        if not execution.is_complete:
            time.sleep(0.5)
            execution.sync()
            if execution.node_executions is not None and len(execution.node_executions) > 1:
                execution.node_executions["n0"]
                # TODO: monitor and inspect node executions
            continue
        else:
            break

    assert execution.outputs.literals["o0"].scalar.primitive.integer == 12
    assert execution.outputs.literals["o1"].scalar.primitive.string_value == "foobarworld"
