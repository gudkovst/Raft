import json
from abc import abstractmethod
from stateMachine import Entry, CommandType
import requests
from pydantic import BaseModel


def json_convert(obj):
    if isinstance(obj, Entry):
        return obj.__dict__
    if isinstance(obj, CommandType):
        return obj.value
    else:
        return str(obj)


class Transport:
    timeout = 0.5

    @abstractmethod
    def send(self, method: str, params, addr, port):
        pass

    def redirect(self, req: str, addr, port):
        pass


class TransportRPC(Transport):

    def redirect(self, req: str, addr, port):
        dest = str(addr) + ":" + str(port)
        url = "http://" + dest + "/" + req
        response = requests.get(url, timeout=self.timeout)
        if response.status_code == 200:
            return response.text
        else:
            return response.status_code

    def send(self, method: str, params, addr, port):
        return self.call_rpc(method, params, addr, port)

    def call_rpc(self, method: str, rpc_params: BaseModel, addr, port):
        dest = str(addr) + ":" + str(port)
        url = "http://" + dest + "/api/v1/jsonrpc"
        headers = {'content-type': 'application/json'}

        loc_json_rpc = {"jsonrpc": "2.0",
                        "id": "0",
                        "method": method,
                        "params": {'in_params': rpc_params.dict()}
                        }

        try:
            response = requests.post(url, data=json.dumps(loc_json_rpc, default=json_convert), headers=headers,
                                     timeout=self.timeout)
        except Exception as e:
            print("No answer from " + dest)
            return {'datas': e.args}

        if response.status_code == 200:
            response = response.json()

            if 'result' in response:
                return response['result']
            else:
                return {'datas': 'error fnc not found'}
        else:
            print('status code is not 200')
            return {'datas': 'error response'}
