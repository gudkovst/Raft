import json
from abc import abstractmethod

import requests
from pydantic import BaseModel


class Transport:

    @abstractmethod
    def send(self, method: str, params, addr, port):
        pass


class TransportRPC(Transport):

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
