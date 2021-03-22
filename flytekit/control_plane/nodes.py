from flytekit.common.mixins import hash as _hash_mixin
from flytekit.models.core import workflow as _workflow_model


class FlyteNode(_hash_mixin.HashOnReferenceMixin, _workflow_model.Node):
    pass
