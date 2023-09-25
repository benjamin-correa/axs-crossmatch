from pathlib import Path

import anyconfig


def load_configuration(_config_path: str = "config/") -> dict[str, any]:
    """Loads a configuration object with content from a folder containing
    YAML files.

    All configurations found in a file are nested in a key with the file name
    Example:
        parameters.yml
            variable: 1

        file.yml
            parameter: x

        will become:

        {'parameters': {'variable': 1}, 'file': {'parameter': 'x'}}

    Args:
        _config_path (str, optional): description. Defaults to 'config/'.

    Returns:
        dict[str, any]: Dictionary with parameters
    """
    # Loading global parameters
    parameters_path = Path(_config_path).joinpath("parameters.yml")
    parameters = anyconfig.load(parameters_path)

    pathlist = Path(_config_path).glob("**/*.yml")

    _config = {}
    for path_obj in pathlist:
        if str(parameters_path) in str(path_obj):
            pass
        file_path = str(path_obj)
        file_name = str(path_obj.stem)
        _config[file_name] = anyconfig.load(
            file_path, ac_template=True, ac_context=parameters
        )
    return _config
