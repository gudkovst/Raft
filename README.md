Простая реализация алгоритма Raft.

[Документация к алгоритму](https://raft.github.io/raft.pdf).

fastapi_jsonrpc можно установить [отсюда](https://github.com/smagafurov/fastapi-jsonrpc).



Запускать так - .\main.py [стартовый порт] [собственный порт] [количество узлов]


Для управления журналом:

[ 
http://localhost:[port]/create?key=key&value=value

[
http://localhost:[port]/read?key=key

[
http://localhost:[port]/update?key=key&value=value

[
http://localhost:[port]/delete?key=key
