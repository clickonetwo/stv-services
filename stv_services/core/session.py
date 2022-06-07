#  MIT License
#
#  Copyright (c) 2022 Daniel C. Brotsky
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in all
#  copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#  SOFTWARE.
#
from typing import ClassVar, Dict

import requests
from pyairtable import Api

from .config import Configuration


class Session:
    sessions: ClassVar[Dict[str, requests.Session]] = {}
    api: ClassVar[Api] = None
    config: ClassVar[Configuration] = None

    @classmethod
    def init_config(cls):
        if not cls.config:
            cls.config = Configuration.get_global_config()

    @classmethod
    def get_global_session(cls, service: str) -> requests.Session:
        cls.init_config()
        service = service.lower()
        session = cls.sessions.get(service)
        if session is None:
            if service == "action_network":
                api_key = cls.config["action_network_api_key"]
                session = requests.session()
                session.headers["OSDI-API-Token"] = api_key
                cls.sessions[service] = session
            elif service == "airtable":
                api_key = cls.config["airtable_api_key"]
                session = requests.session()
                session.headers["Authorization"] = f"Bearer {api_key}"
                cls.sessions[service] = session
            elif service == "mobilize":
                api_key = cls.config["mobilize_api_key"]
                session = requests.session()
                session.headers["Authorization"] = f"Bearer {api_key}"
            else:
                raise ValueError(
                    f"no session available because '{service}' is not a known service"
                )
        return session

    @classmethod
    def get_airtable_api(cls) -> Api:
        cls.init_config()
        if cls.api is None:
            api_key = cls.config["airtable_api_key"]
            cls.api = Api(api_key, timeout=(3.5, 27))
        return cls.api
