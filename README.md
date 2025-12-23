# Vacancy Forwarder

Forward incoming Telegram vacancies (VK / Yandex.Direct only) to a channel.
See config.example.json -> copy to config.json and edit.

Quick start (server):

1. clone repo
2. python -m venv venv
3. ./venv/bin/pip install -r requirements.txt
4. edit config.json
5. ./venv/bin/python forwarder.py          # runs import + live
6. (optional) configure systemd unit from systemd/vacancy_forwarder.service

See README for full commands and instructions.
