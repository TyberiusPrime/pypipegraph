
class PyPipelineGraphError(ValueError):
    pass

class CycleError(PyPipelineGraphError):
    pass

class RuntimeError(PyPipelineGraphError):
    pass

class JobContractError(PyPipelineGraphError):
    pass
