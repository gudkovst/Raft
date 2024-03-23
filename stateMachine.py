from enum import Enum


class CommandType(Enum):
    CREATE = 0
    READ = 1
    UPDATE = 2
    DELETE = 3


class Entry:

    def __init__(self, cmd: CommandType, term: int, key, value):
        self.cmd = cmd
        self.term = term
        self.key = key
        self.value = value


class StateMachine:
    storage = dict()

    def create(self, key, value):
        if key in self.storage:
            return False
        self.storage[key] = value
        return True

    def read(self, key, value):
        return self.storage.get(key, False)

    def update(self, key, value):
        if key not in self.storage:
            return False
        self.storage[key] = value
        return True

    def delete(self, key, value):
        return self.storage.pop(key, False)

    def apply(self, entry: Entry):
        try:
            func = getattr(self, entry.cmd.name.lower())
        except AttributeError:
            return False
        return func(entry.key, entry.value)
