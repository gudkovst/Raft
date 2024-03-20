import random
import time
from enum import Enum
from threading import Timer

from messages import *
from transportLayer import TransportRPC


class State(Enum):
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 2


class Entry:

    def __init__(self, cmd, term: int):
        self.cmd = cmd
        self.term = term


class RepeatTimer(Timer):
    def run(self):
        while not self.finished.wait(self.interval):
            self.function(*self.args, **self.kwargs)


class MySyncObj:
    loc_ip = "localhost"
    port: int = None
    other_ports = []

    cur_state = State.FOLLOWER
    cur_term = 0
    voted_for = None
    log: list[Entry] = []
    commit_index = 0
    last_applied = 0
    next_index = []
    match_index = []

    heartbeat = False
    timestamp = None
    timer = None
    election_timeout = None
    transport = TransportRPC()

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
        last_p = int(starting_p) + int(amount)
        for p in range(int(starting_p), last_p):
            if p != int(self_p):
                self.other_ports.append(p)

    def quorum(self, voted: int) -> bool:
        return voted > len(self.other_ports) / 2

    def do_work(self):
        now = time.time()
        delay = now - self.timestamp
        # print("Delay: " + str(int(delay / 60)) + " minutes; " + str(int(delay % 60)) + " seconds")
        if self.cur_state == State.LEADER:
            self.updTM()
            for port in self.other_ports:
                res = self.transport.send("append_entries",
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
            last_log_term = self.log[-1].term if len(self.log) else 0
            msg = VoteInDataModel(term=self.cur_term, candidate_id=self.port, last_log_index=len(self.log),
                                  last_log_term=last_log_term)
            response = self.transport.send("request_vote", msg, self.loc_ip, port)
            try:
                if int(response["term"]) > self.cur_term:
                    self.cur_term = int(response["term"])
                    self.voted_for = None
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
                self.transport.send("append_entries", AppendInDataModel(term=self.cur_term, leader_id=str(self.port)),
                                    self.loc_ip, port)
            self.next_index = [len(self.log) + 1] * (len(self.other_ports) + 1)
            self.match_index = [0] * (len(self.other_ports) + 1)
        self.heartbeat = True
        self.set_election_tm(success_elec=False)

    def request_vote_handler(self, in_params: VoteInDataModel) -> VoteOutDataModel:
        self.updTM()
        vote_granted = False
        print("Vote request from " + str(in_params.candidate_id) + " to " + str(self.port))
        if self.cur_term <= in_params.term and self.voted_for in [None, in_params.candidate_id] and \
                len(self.log) <= in_params.last_log_index:
            vote_granted = True
            self.voted_for = in_params.candidate_id
            self.cur_term = in_params.term
            self.cur_state = State.FOLLOWER
            print("VOTED FOR " + " " + str(in_params.candidate_id))
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
