from pydantic import BaseModel

import fastapi_jsonrpc as jsonrpc


class MyError(jsonrpc.BaseError):
    CODE = 5000
    MESSAGE = 'My error'

    class DataModel(BaseModel):
        details: str


class VoteInDataModel(BaseModel):
    term: int
    candidate_id: int
    last_log_index: int
    last_log_term: int


class VoteOutDataModel(BaseModel):
    term: int
    vote_granted: bool


class AppendInDataModel(BaseModel):
    term: int
    leader_id: int
    prev_log_index: int
    prev_log_term: int
    entries: list
    leader_commit: int


class AppendOutDataModel(BaseModel):
    term: int
    success: bool
    commit_index: int
