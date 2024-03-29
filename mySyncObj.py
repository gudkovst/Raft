import random
import time
from threading import Timer

from messages import *
from stateMachine import *
from transportLayer import TransportRPC


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
    port: int = None
    other_ports = []
    start_port = None

    cur_state = State.FOLLOWER
    cur_term = 0
    voted_for = None
    log: list[Entry] = []
    commit_index = 0
    last_applied = 0
    next_index = []
    match_index = []

    state_machine = None
    heartbeat = False
    timestamp = None
    timer = None
    election_timeout = None
    transport = TransportRPC()

    def __init__(self):
        self.timer = RepeatTimer(5.0, self.do_work)
        self.state_machine = StateMachine()

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
        self.start_port = int(starting_p)
        last_p = self.start_port + int(amount)
        for p in range(self.start_port, last_p):
            if p != int(self_p):
                self.other_ports.append(p)

    def quorum(self, voted: int) -> bool:
        return voted > len(self.other_ports) / 2

    def send_heartbeat(self, to: int):
        pr_log_term = self.log[-1].term if len(self.log) else 0
        msg = AppendInDataModel(term=self.cur_term, leader_id=self.port, prev_log_index=len(self.log),
                                prev_log_term=pr_log_term, entries=[], leader_commit=self.commit_index)
        self.transport.send("append_entries", msg, self.loc_ip, to)

    def do_work(self):
        now = time.time()
        delay = now - self.timestamp
        # print("Delay: " + str(int(delay / 60)) + " minutes; " + str(int(delay % 60)) + " seconds")
        if self.commit_index > self.last_applied:  # TODO respond client after apply from leader
            self.state_machine.apply(self.log[self.last_applied])
            self.last_applied += 1

        if self.cur_state == State.LEADER:
            self.updTM()
            for port in self.other_ports:
                follower_num = port - self.start_port
                entries = self.log[self.next_index[follower_num]:]
                if not entries:
                    self.send_heartbeat(port)
                    continue
                msg = AppendInDataModel(term=self.cur_term, leader_id=self.port, prev_log_index=self.next_index[follower_num],
                                        prev_log_term=entries[0].term, entries=entries, leader_commit=self.commit_index)
                res = self.transport.send("append_entries", msg, self.loc_ip, port)
                print('append res', res)
                try:
                    if int(res["term"]) > self.cur_term:
                        self.cur_term = int(res["term"])
                        self.voted_for = None
                        self.cur_state = State.FOLLOWER
                    elif bool(res["success"]):
                        self.next_index[follower_num] = len(self.log)
                        self.match_index[follower_num] = len(self.log)
                    else:
                        self.next_index[follower_num] -= 1
                except Exception:
                    continue
            for N in range(len(self.log), self.commit_index, -1):
                k_repl = sum([m >= N for m in self.match_index])
                if self.quorum(k_repl) and self.log[N - 1].term == self.cur_term:
                    self.commit_index = N
                    break

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
                self.send_heartbeat(port)
            self.next_index = [len(self.log)] * (len(self.other_ports) + 1)
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
        print("Append request from " + str(in_params.leader_id) + " to " + str(self.port) + " with term " + str(in_params.term))
        if self.cur_term <= in_params.term:
            self.cur_state = State.FOLLOWER
            self.heartbeat = True
            self.cur_term = in_params.term
            self.voted_for = None
        if self.cur_term > in_params.term or in_params.prev_log_index > len(self.log):  # 1 and 2 rec impl
            return AppendOutDataModel(term=self.cur_term, success=False)
        for i, entry in enumerate(in_params.entries):  # 3 and 4 rec impl
            if in_params.prev_log_index + i < len(self.log):
                self.log[in_params.prev_log_index + i] = entry
            else:
                self.log.append(entry)
        if in_params.leader_commit > self.commit_index:  # 5 rec impl
            self.commit_index = min(in_params.leader_commit, len(self.log))
        return AppendOutDataModel(term=self.cur_term, success=True)

    def start(self):
        self.cur_state = State.FOLLOWER
        self.updTM()
        self.set_election_tm()
        self.timer.start()
