import time
from enum import Enum


class CommandType(Enum):
    CREATE = 0
    READ = 1
    UPDATE = 2
    DELETE = 3
    CAS = 4
    READ_MUTEX = 5


class Entry:

    def __init__(self, cmd: CommandType, term: int, key, value=None, old_value=None):
        self.cmd = cmd
        self.term = term
        self.key = key
        self.value = value
        self.old_value = old_value


class Mutex:

    def __init__(self):
        self.value = "0"
        self.time = 0

    @property
    def value(self):
        return self.__value

    @value.setter
    def value(self, new_value: str):
        self.__value = new_value
        self.time = time.time()

    def is_dead(self, t: int) -> bool:
        return int(self.value) and time.time() - self.time > t


class StateMachine:
    storage = dict()
    mutex = Mutex()

    def create(self, entry: Entry):
        if entry.key in self.storage:
            return False
        self.storage[entry.key] = entry.value
        return True

    def read(self, entry: Entry):
        return self.storage.get(entry.key, False)

    def update(self, entry: Entry):
        if entry.key not in self.storage:
            return False
        self.storage[entry.key] = entry.value
        return True

    def delete(self, entry: Entry):
        return self.storage.pop(entry.key, False)

    def cas(self, entry: Entry):
        value = self.mutex.value
        if value != entry.old_value:
            return False
        self.mutex.value = entry.value
        return True

    def read_mutex(self, *args):
        return self.mutex.value

    def apply(self, entry: Entry):
        try:
            func = getattr(self, entry.cmd.name.lower())
        except AttributeError:
            return False
        return func(entry)
