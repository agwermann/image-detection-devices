# Edge Service

## Setup Virtual environment

```bash
python3 -m venv venv
source venv/bin/activate
```

## Install Dependencies

```bash
pip3 install -r requirements.txt
```

## Start Locally

```bash
python3 publisher.py mytopic 10 1
python3 publish-workload.py mytopic 10 1 10 0.1
```
