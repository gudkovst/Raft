import json
import random
import sys
import time
from contextlib import asynccontextmanager
from enum import Enum
from threading import Timer

import requests
import uvicorn
from pydantic import BaseModel
from messages import *
import fastapi_jsonrpc as jsonrpc


class State(Enum):
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 2


class RepeatTimer(Timer):
    def run(self):
        while not self.finished.wait(self.interval):
            self.function(*self.args, **self.kwargs)


class MySyncObj:
    loc_ip = "localhost"
    other_ports = []
    cur_state = State.FOLLOWER
    port = None
    port_for_rpc = None
    heartbeat = False
    cur_term = 0
    voted_for = None
    timestamp = None
    timer = None
    election_timeout = None
    loc_index = 0

    def __init__(self):
        self.timer = RepeatTimer(5.0, self.do_work)

    def updTM(self):
        self.timestamp = time.time()

    def getTM(self):
        return self.timestamp

    def set_election_tm(self, success_elec: bool = True):
        if success_elec:
            self.election_timeout = 5 + random.randint(10, 30) * 0.01
        else:
            self.election_timeout = random.randint(150, 300) / 1000

    def init_address(self, starting_p, self_p, amount):
        self.port = int(self_p)
        for p in range(int(starting_p), int(amount)):
            if p != int(self_p):
                self.other_ports.append(p)
        self.port_for_rpc = str(self.other_ports[0])

    def quorum(self, voted: int) -> bool:
        return voted > len(self.other_ports) / 2

    def call_rpc(self, method: str, rpc_params: BaseModel, addr: str = None, port: str = None):
        port = port or self.port_for_rpc
        addr = addr or self.loc_ip
        dest = str(addr) + ":" + str(port)
        url = "http://" + dest + "/api/v1/jsonrpc"
        headers = {'content-type': 'application/json'}

        loc_json_rpc = {"jsonrpc": "2.0",
                        "id": "0",
                        "method": method,
                        "params": {'in_params': rpc_params.dict()}
                        }

        try:
            response = requests.post(url, data=json.dumps(loc_json_rpc), headers=headers, timeout=0.5)
        except Exception:
            print("No answer from " + dest)
            return {'datas': 'error connection'}

        if response.status_code == 200:
            response = response.json()

            if 'result' in response:
                return response['result']
            else:
                return {'datas': 'error fnc not found'}
        else:
            print('status code is not 200')
            return {'datas': 'error response'}

    def do_work(self):
        now = time.time()
        delay = now - self.timestamp
        # print("Delay: " + str(int(delay / 60)) + " minutes; " + str(int(delay % 60)) + " seconds")
        if self.cur_state == State.LEADER:
            self.updTM()
            for port in self.other_ports:
                res = self.call_rpc("append_entries",
                                    AppendInDataModel(term=self.cur_term, leader_id=str(self.port)),
                                    self.loc_ip, port)
                print('append res', res)
        elif self.cur_state == State.FOLLOWER:
            if delay > self.election_timeout and not self.heartbeat:
                self.cur_state = State.CANDIDATE
                self.set_election_tm()
                self.election()
            elif self.heartbeat:
                self.heartbeat = False

    def election(self):
        self.cur_term += 1
        self.voted_for = self.port
        votes = 1
        for port in self.other_ports:
            response = self.call_rpc("request_vote",
                                     VoteInDataModel(term=self.cur_term, candidate_id=str(self.port)),
                                     self.loc_ip, port)
            try:
                if int(response["term"]) > self.cur_term:
                    self.cur_term = int(response["term"])
                    self.cur_state = State.FOLLOWER
                    return
                if bool(response["vote_granted"]):
                    votes += 1
                    if self.quorum(votes):
                        break
            except Exception:
                continue
        if self.quorum(votes):
            self.cur_state = State.LEADER
            print("IM LEADER")
            for port in self.other_ports:
                self.call_rpc("append_entries", AppendInDataModel(term=self.cur_term, leader_id=str(self.port)),
                              self.loc_ip, port)
        self.heartbeat = True
        self.set_election_tm(success_elec=False)

    def request_vote_handler(self, in_params: VoteInDataModel) -> VoteOutDataModel:
        self.updTM()
        vote_granted = False
        print("Vote request from " + in_params.candidate_id + " to " + str(self.port))
        if self.cur_term <= in_params.term:
            vote_granted = True
            self.voted_for = in_params.candidate_id
            self.cur_term = in_params.term
            print("VOTED FOR " + " " + in_params.candidate_id)
        return VoteOutDataModel(term=self.cur_term, vote_granted=vote_granted)

    def append_entries_handler(self, in_params: AppendInDataModel) -> AppendOutDataModel:
        self.updTM()
        success = False
        print("Append request from " + in_params.leader_id + " to " + str(self.port))
        if self.cur_term <= in_params.term:
            self.cur_state = State.FOLLOWER
            success = True
            self.heartbeat = True
            self.cur_term = in_params.term
        return AppendOutDataModel(term=self.cur_term, success=success)

    def start(self):
        self.cur_state = State.FOLLOWER
        self.updTM()
        self.set_election_tm()
        self.timer.start()
