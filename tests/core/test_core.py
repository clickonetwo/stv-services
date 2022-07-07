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
import pytest
import sqlalchemy as sa
from sqlalchemy.future import Connection

from stv_services.core import Session
from stv_services.core.config import Configuration
from stv_services.data_store import Postgres, model


def test_load_config():
    config = Configuration()
    assert isinstance(config, dict)
    assert len(config) == 0
    config.load_from_data_store()
    assert config.get("Test Key Not in Real Configs") is None


def test_save_config():
    test_key = "Test Key Not in Real Configs"
    test_key2 = "Another test key not in Real Configs"
    test_val = {"test value key": "test value value"}
    test_val_modified = {"test value key": "modified test value value"}
    test_val2 = "Another test value"
    config0 = Configuration.get_global_config()
    assert config0.get(test_key) is None
    config0[test_key] = test_val
    assert config0.original.get(test_key) is None
    config0.save_to_data_store()
    assert config0.original.get(test_key) == test_val
    assert config0.original.get(test_key) is not test_val
    config0[test_key]["test value key"] = "modified test value value"
    assert config0[test_key] == test_val_modified
    assert config0.original.get(test_key) != test_val_modified
    config0.save_to_data_store()
    assert config0.original.get(test_key) == test_val_modified
    with Postgres.get_global_engine().connect() as conn:  # type: Connection
        config1 = Configuration.get_session_config(conn)
        assert config1.get(test_key) == test_val_modified
        del config1[test_key]
        assert config1.original.get(test_key) == test_val_modified
        config1.save_to_connection(conn)
        assert test_key not in config1.original
        conn.commit()
    with Postgres.get_global_engine().connect() as conn:  # type: Connection
        config1 = Configuration.get_session_config(conn)
        assert test_key not in config1
    config2 = Configuration.get_global_config()
    assert config2 is config0
    config2[test_key2] = test_val2
    config3 = Configuration.get_global_config(reload=True)
    assert config3 is config2
    assert test_key2 not in config3.original
    assert test_key in config3
    assert config3.get(test_key2) == test_val2
    config3.save_to_data_store()
    assert config3.original.get(test_key2) == test_val2
    del config3[test_key]
    del config3[test_key2]
    config3.save_to_data_store()
    assert config3 is config2
    assert config3 is config0
    assert test_key2 not in config2
    assert test_key2 not in config0
    assert test_key not in config3


def test_get_action_network_session():
    session = Session.get_global_session("action_network")
    assert session is not None


@pytest.mark.skip("Don't clear database during routine testing")
def test_clear_database():
    Postgres.clear_importable_data()
    with Postgres.get_global_engine().connect() as conn:
        for table in [
            model.person_info,
            model.donation_info,
            model.submission_info,
            model.fundraising_page_info,
        ]:
            result = conn.execute(sa.select(table)).mappings().all()
            assert len(result) == 0
