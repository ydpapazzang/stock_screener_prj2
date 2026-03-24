from __future__ import annotations

from importlib import resources


def resource_filename(package: str, resource_name: str) -> str:
    return str(resources.files(package).joinpath(resource_name))
