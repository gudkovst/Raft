import sys
from contextlib import asynccontextmanager

import uvicorn

from mySyncObj import *

api_v1 = jsonrpc.Entrypoint('/api/v1/jsonrpc')


@api_v1.method(errors=[MyError])
def request_vote(in_params: VoteInDataModel) -> VoteOutDataModel:
    return my_raft.request_vote_handler(in_params)


@api_v1.method(errors=[MyError])
def append_entries(in_params: AppendInDataModel) -> AppendOutDataModel:
    return my_raft.append_entries_handler(in_params)


@api_v1.method(errors=[MyError])
def create(key, value):
    if my_raft.cur_state == State.LEADER:
        entry = Entry(CommandType.CREATE, my_raft.cur_term, key, value)
        my_raft.log.append(entry)
        return True
    else:
        return my_raft.redirect(f"create?key={key}&value={value}")


@api_v1.method(errors=[MyError])
def read(key):
    if my_raft.cur_state == State.LEADER:
        entry = Entry(CommandType.READ, my_raft.cur_term, key)
        res = my_raft.state_machine.apply(entry)
        return res
    else:
        return my_raft.redirect(f"read?key={key}")


@api_v1.method(errors=[MyError])
def update(key, value):
    if my_raft.cur_state == State.LEADER:
        entry = Entry(CommandType.UPDATE, my_raft.cur_term, key, value)
        my_raft.log.append(entry)
        return True
    else:
        return my_raft.redirect(f"update?key={key}&value={value}")


@api_v1.method(errors=[MyError])
def delete(key):
    if my_raft.cur_state == State.LEADER:
        entry = Entry(CommandType.DELETE, my_raft.cur_term, key)
        my_raft.log.append(entry)
        return True
    else:
        return my_raft.redirect(f"delete?key={key}")


@api_v1.method(errors=[MyError])
def cas(old_value, new_value):
    if my_raft.cur_state == State.LEADER:
        my_raft.lock_cas.acquire()
        res = False
        entry = Entry(CommandType.CAS, my_raft.cur_term, "__mutex", new_value, old_value)
        if not my_raft.cas_in_log and my_raft.state_machine.can_cas(entry):
            my_raft.log.append(entry)
            my_raft.cas_in_log = True
            res = True
        my_raft.lock_cas.release()
        return res
    else:
        return my_raft.redirect(f"cas?old_value={old_value}&new_value={new_value}")


@api_v1.method(errors=[MyError])
def read_mutex():
    if my_raft.cur_state == State.LEADER:
        entry = Entry(CommandType.READ_MUTEX, my_raft.cur_term, "__mutex")
        return my_raft.state_machine.apply(entry)
    else:
        return my_raft.redirect(f"read_mutex")


@api_v1.method(errors=[MyError])
def unlock(node):
    return my_raft.redirect(f"cas?old_value={node}&new_value=0")


@api_v1.method(errors=[MyError])
def crit_sec():
    my_raft.applier.submit(my_raft.critical_section)
    return True


if __name__ == '__main__':
    my_raft = MySyncObj()
    args = sys.argv[1:]
    my_raft.init_address(args[0], args[1], args[2])


    @asynccontextmanager
    async def lifespan(app: jsonrpc.API):
        my_raft.start()
        yield


    app = jsonrpc.API(lifespan=lifespan)

    app.bind_entrypoint(api_v1)
    app.add_api_route("/request_vote_rpc", request_vote, methods=["GET"])
    app.add_api_route("/append_entries_rpc", append_entries, methods=["GET"])
    app.add_api_route("/create", create, methods=["GET"])
    app.add_api_route("/read", read, methods=["GET"])
    app.add_api_route("/update", update, methods=["GET"])
    app.add_api_route("/delete", delete, methods=["GET"])
    app.add_api_route("/cas", cas, methods=["GET"])
    app.add_api_route("/crit_sec", crit_sec, methods=["GET"])
    app.add_api_route("/read_mutex", read_mutex, methods=["GET"])
    app.add_api_route("/unlock", unlock, methods=["GET"])

    uvicorn.run(app, host=my_raft.loc_ip, port=my_raft.port, log_level="critical")
