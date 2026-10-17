"""A detached launcher must not take the storage data plane down with its logs."""
import http.client
import os
from pathlib import Path
import socket
import subprocess
import time

import pytest


def test_data_plane_survives_closed_stdout_and_stderr(tmp_path):
    binary = os.environ.get('ALOCALS3_SERVER_BIN')
    if not binary:
        pytest.skip('set ALOCALS3_SERVER_BIN to the built server')
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', 0))
        port = listener.getsockname()[1]
    process = subprocess.Popen([binary, '--host', '127.0.0.1', '--port', str(port),
        '--database-url', 'sqlite:///' + str(tmp_path / 'metadata.db'),
        '--storage-root', str(tmp_path / 'objects'), '--disable-gc', '--auth-enabled=false'],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        env={key: value for key, value in os.environ.items() if not key.startswith('ALOCALS3_')})
    def get(path):
        connection = http.client.HTTPConnection('127.0.0.1', port, timeout=3)
        try:
            connection.request('GET', path)
            response = connection.getresponse()
            response.read()
            return response.status
        finally:
            connection.close()
    try:
        deadline = time.monotonic() + 10
        while True:
            try:
                assert get('/healthz') == 200
                break
            except OSError:
                if time.monotonic() > deadline:
                    raise
                time.sleep(.025)
        process.stdout.close()
        for _ in range(35):
            assert get('/s3') == 200
            assert get('/api/object/missing-bucket/missing-object') == 404
            # Include a sampled health log, rather than relying on the quiet path.
            assert get('/healthz') == 200
    finally:
        if process.stdout:
            process.stdout.close()
        process.terminate()
        process.wait(timeout=10)
