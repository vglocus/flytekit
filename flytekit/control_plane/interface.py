from flytekit.models import interface as _interface_models


class TypedInterface(_interface_models.TypedInterface):
    @classmethod
    def promote_from_model(cls, model):
        """
        :param flytekit.models.interface.TypedInterface model:
        :rtype: TypedInterface
        """
        return cls(model.inputs, model.outputs)

    def create_bindings_for_inputs(self, map_of_bindings):
        # TODO
        pass

    def __repr__(self):
        # TODO
        pass
