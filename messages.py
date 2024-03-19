from pydantic import BaseModel

import fastapi_jsonrpc as jsonrpc


class MyError(jsonrpc.BaseError):
    CODE = 5000
    MESSAGE = 'My error'

    class DataModel(BaseModel):
        details: str


class VoteInDataModel(BaseModel):
    term: float
    candidate_id: str


class VoteOutDataModel(BaseModel):
    term: float
    vote_granted: bool


class AppendInDataModel(BaseModel):
    term: float
    leader_id: str


class AppendOutDataModel(BaseModel):
    term: float
    success: bool
