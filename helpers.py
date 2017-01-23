#!/usr/bin/env python3
# -*- coding: utf-8 -*-


def prepare_module_message(message: str) -> str:
    if message.find('"') >= 0:
        return message

    message = message.replace('{', '{"')
    message = message.replace('}', '"}')
    message = message.replace(':', '":"')
    message = message.replace(',', '","')
    message = message.replace('"[', '[')
    message = message.replace(']"', ']')
    return message.replace('}","{', '},{')


def form_key(nid: str, mal='') -> str:
    return nid + ('/' + mal if mal else '')


def from_key(key: str) -> list:
    return key.split('/')
