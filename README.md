Connect to dump1090 basestation port and persist messages to a REDIS data store

# Install project with uv
```bash
uv venv
uv sync
cp config.py.template config.py
```
Edit config.py - set REDIS login username/host/port/password
Update the hostname and port of the dump1090/Flight Aware host
