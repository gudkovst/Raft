import sys
from contextlib import asynccontextmanager

import uvicorn
from stateMachine import Entry, CommandType
from messages import *
from mySyncObj import MySyncObj

api_v1 = jsonrpc.Entrypoint('/api/v1/jsonrpc')


@api_v1.method(errors=[MyError])
async def request_vote(in_params: VoteInDataModel) -> VoteOutDataModel:
    return await my_raft.request_vote_handler(in_params)


@api_v1.method(errors=[MyError])
async def append_entries(in_params: AppendInDataModel) -> AppendOutDataModel:
    return await my_raft.append_entries_handler(in_params)


@api_v1.method(errors=[MyError])
def create(key, value):
    entry = Entry(CommandType.CREATE, my_raft.cur_term, key, value)
    my_raft.log.append(entry)
    print("CREATE " + my_raft.log[-1].key)


@api_v1.method(errors=[MyError])
def read(key, value):
    entry = Entry(CommandType.READ, my_raft.cur_term, key, value)
    res = my_raft.state_machine.apply(entry)
    return res


@api_v1.method(errors=[MyError])
def update(key, value):
    entry = Entry(CommandType.UPDATE, my_raft.cur_term, key, value)
    my_raft.log.append(entry)


@api_v1.method(errors=[MyError])
def delete(key, value):
    entry = Entry(CommandType.DELETE, my_raft.cur_term, key, value)
    my_raft.log.append(entry)


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

    uvicorn.run(app, host=my_raft.loc_ip, port=my_raft.port, log_level="critical")
