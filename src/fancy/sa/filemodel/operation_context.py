from typing import Optional

from . import operation


class OperationContext:
    _operations: list[operation.Operation]
    _parent: "OperationContext" = None
    committed: bool = False
    rolled_back: bool = False

    def __init__(self, parent: "OperationContext" = None):
        self._operations = []
        self._parent = parent

    def add(self, op: operation.Operation) -> None:
        op.on_attach_context()
        self._operations.append(op)

    def commit(self) -> None:
        if self.is_top():
            for op in self._operations:
                op.do()
        else:
            self._parent._operations += self._operations

    def rollback(self) -> None:
        for op in self._operations:
            op.undo()

    def is_top(self) -> bool:
        return self._parent is None

    def get_parent(self) -> Optional["OperationContext"]:
        return self._parent
