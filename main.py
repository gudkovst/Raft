import sys
from contextlib import asynccontextmanager

import uvicorn

from messages import *
from mySyncObj import MySyncObj

api_v1 = jsonrpc.Entrypoint('/api/v1/jsonrpc')


@api_v1.method(errors=[MyError])
def request_vote(in_params: VoteInDataModel) -> VoteOutDataModel:
    return my_raft.request_vote_handler(in_params)


@api_v1.method(errors=[MyError])
def append_entries(in_params: AppendInDataModel) -> AppendOutDataModel:
    return my_raft.append_entries_handler(in_params)


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
    uvicorn.run(app, host=my_raft.loc_ip, port=my_raft.port, log_level="critical")
