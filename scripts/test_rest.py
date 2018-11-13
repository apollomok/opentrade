#!/usr/bin/env python

import json
import requests

url = 'http://127.0.0.1:9111/api'


def test():
  login = ['login', "test", 'test']
  res = requests.post(url, data=json.dumps(login))
  res = json.loads(res.text)
  print(res)
  if res[1] == 'ok':
    session_token = res[2]['sessionToken']
  else:
    return
  print(session_token)
  securities = ['securities']
  res = requests.post(
      url,
      data=json.dumps(securities),
      headers={
          'session-token': session_token
      })
  res = json.loads(res.text)
  print(res)


if __name__ == '__main__':
  test()
