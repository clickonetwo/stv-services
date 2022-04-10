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
from stv_services.core import Session
from stv_services.core.config import Configuration


def test_load_config():
    config = Configuration()
    assert isinstance(config, dict)
    assert len(config) == 0
    config.load_from_data_store()
    assert config.get("Test Key Not in Real Configs") is None


def test_save_config():
    test_key = "Test Key Not in Real Configs"
    test_val = "Value for Test Key Not in Real Configs"
    config0 = Configuration.get_global_config()
    assert config0.get(test_key) is None
    config0[test_key] = test_val
    config0.save_to_data_store()
    config1 = Configuration.get_global_config()
    assert config1.get(test_key) == test_val
    del config1[test_key]
    config1.save_to_data_store()
    config2 = Configuration.get_global_config()
    config2.load_from_data_store()
    assert config2.get(test_key) is None
    assert config2 == config0


def test_get_action_network_session():
    session = Session.get_global_session("action_network")
    assert session is not None
