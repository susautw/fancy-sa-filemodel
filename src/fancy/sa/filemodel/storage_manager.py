from copy import copy

from sqlalchemy import event, inspect
from sqlalchemy.orm import Session, SessionTransaction

from . import Storage, FileModel, OperationContext, operation


class _SessionMetadata:
    context_stack: OperationContext = None  # A linked-list stack of OperationContext


class StorageManager:
    _session_metadata: dict[int, _SessionMetadata]
    _storage: Storage

    INSTANCE = None

    def __init__(self, storage: Storage):
        self._storage = storage
        self._session_metadata = {}

        self._bind_events = [
            # (callable, target, propagate)
            (self.after_transaction_create, Session, False),
            (self.before_insert, FileModel, True),
            (self.before_update, FileModel, True),
            (self.after_delete, FileModel, True),
            (self.after_commit, Session, False),
            (self.after_rollback, Session, False),
            (self.after_transaction_end, Session, False)
        ]

        for event_, cls_, propagate in self._bind_events:
            event.listen(cls_, event_.__name__, event_, propagate=propagate)

    def after_transaction_create(self, session: Session, transaction: SessionTransaction) -> None:
        if transaction.parent is None or transaction.nested:
            meta = self._get_session_metadata(session)
            meta.context_stack = OperationContext(parent=meta.context_stack)

    def before_insert(self, _mapper, _connection, target: FileModel) -> None:
        meta = self._get_session_metadata(inspect(target).session)
        # regenerate filename
        if (filename := target.generate_filename()) != target.file:
            target.file = copy(target.file)
            target.file.rename(filename)
        meta.context_stack.add(operation.Insert(target.file, self._storage))

    def before_update(self, _mapper, _connection, target: FileModel) -> None:
        if (new_name := target.generate_filename()) != target.file.get_name():
            meta = self._get_session_metadata(inspect(target).session)
            meta.context_stack.add(operation.Update(target.file, new_name, self._storage))

    def after_delete(self, _mapper, _connection, target: FileModel):
        meta = self._get_session_metadata(inspect(target).session)
        meta.context_stack.add(operation.Delete(target.file, self._storage))

    def after_commit(self, session: Session):
        meta = self._get_session_metadata(session)
        meta.context_stack.committed = True

    def after_rollback(self, session: Session):
        meta = self._get_session_metadata(session)
        meta.context_stack.rolled_back = True

    def after_transaction_end(self, session: Session, transaction: SessionTransaction) -> None:
        if transaction.parent is None or transaction.nested:
            meta = self._get_session_metadata(session)
            if meta.context_stack.rolled_back:
                meta.context_stack.rollback()
            elif meta.context_stack.committed:
                meta.context_stack.commit()
            self.pop_context(meta, session)

    def pop_context(self, meta: _SessionMetadata, session: Session):
        if meta.context_stack.is_top():
            del self._session_metadata[session.hash_key]
        meta.context_stack = meta.context_stack.get_parent()

    def _get_session_metadata(self, session: Session) -> _SessionMetadata:
        return self._session_metadata.setdefault(session.hash_key, _SessionMetadata())

    @property
    def storage(self) -> Storage:
        return self._storage

    @classmethod
    def get_instance(cls) -> "StorageManager":
        if cls.INSTANCE is None:
            raise RuntimeError("Invoke StorageManager.get_instance before register a storage")
        return cls.INSTANCE

    @classmethod
    def initialize(cls, storage: Storage) -> None:
        if cls.INSTANCE is not None:
            raise RuntimeError("StorageManager should not be initialized multiple times.")
        cls.INSTANCE = cls(storage)

    def __del__(self):
        for event_, cls_, propagate in self._bind_events:
            event.remove(cls_, event_.__name__, event_)
