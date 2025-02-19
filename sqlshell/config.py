"""
Configuration classes for sqlshell. Separated, to reduce code clutter in
the main module.
"""

from dataclasses import dataclass
import os
from pathlib import Path
from string import Template
import tomllib
from typing import Any, Self


class ConfigurationError(Exception):
    """
    Thrown to indicate a configuration error.
    """


@dataclass(frozen=True)
class ConnectionConfig:
    """
    A single connection configuration from the configuration file.
    """

    name: str
    url: str
    history_file: Path | None


class Configuration:
    """
    Represents the parsed configuration data.
    """

    def __init__(
        self: Self, configs: list[ConnectionConfig], path: Path
    ) -> None:
        """
        Initialize a Configuration object.
        """
        self._configs = configs
        self._path = path

    @property
    def path(self: Self) -> Path:
        """
        Returns the path associated with the configuration.
        """
        return self._path

    def lookup(self: Self, spec: str) -> list[ConnectionConfig] | None:
        """
        Uses a string to look up a configuration. Returns a list of matching
        configurations, or None if no match.
        """
        matches = [
            c for c in self._configs if c.name.lower().startswith(spec.lower())
        ]

        if len(matches) == 0:
            return None

        return matches


class EnvDict(dict):
    """
    For environment substitution, we want a reference to a non-existent
    variable to substitute "", rather than throw an error (as with
    Template.substitute()) or leave the reference intact (as with
    Template.safe_substitute()). To do that, we simply use a custom
    dictionary class.
    """

    def __init__(self: Self, *args, **kw) -> None:
        """Initialize the dictionary"""
        self.update(*args, **kw)

    def __getitem__(self: Self, key: Any) -> Any:
        """Get an item from the dictionary"""
        return super().get(key, "")


def load_configuration(config: Path) -> Configuration | None:
    """
    Reads the configuration file, if it exists. Returns a dictionary
    where the keys are names (sections) from the configuration and the
    values are ConnectionConfig objects. The dictionary will be empty,
    if there is no configuration file or if the configuration file is
    empty. Raises ConfigurationError on error.

    :param config: Path to the configuration file, which does not have to
        exist
    """
    assert config.exists()

    try:
        with open(config, mode="rb") as f:
            data = tomllib.load(f)
    except Exception as e:
        # pylint: disable=raise-missing-from
        raise ConfigurationError(f'Unable to read "{config}": {e}')

    env = EnvDict(**os.environ)

    configs: list[ConnectionConfig] = []
    # Loop over each section and substitute environment variables and expand
    # "~" home directory markers in appropriate values.
    for key, values in data.items():
        url = values.get("url")
        if url is None:
            raise ConfigurationError(
                f'"{config}": Section "{key}" has no "url" setting.'
            )

        t = Template(url)
        url = t.substitute(env)

        history = values.get("history")
        if history is not None:
            t = Template(history)
            history = Path(t.substitute(env)).expanduser()

        configs.append(
            ConnectionConfig(name=key, url=url, history_file=history)
        )

    return Configuration(configs=configs, path=config)
