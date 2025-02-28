import os
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import ast
from collections import defaultdict
import json
import re   

from invoke import task

from dbt_invoke.internal import _utils

_LOGGER = _utils.get_logger('dbt-invoke')
_MACRO_NAME = '_log_columns_list'
_SUPPORTED_RESOURCE_TYPES = {
    'model': 'models',
    'seed': 'seeds',
    'snapshot': 'snapshots',
    'analysis': 'analyses',
}
_PROGRESS_PADDING = 9  # Character padding to align progress logs

_update_and_delete_help = {
    arg.replace('_', '-'): details['help']
    for arg, details in _utils.DBT_LS_ARGS.items()
}
_update_and_delete_help['log-level'] = (
    "One of Python's standard logging levels"
    " (DEBUG, INFO, WARNING, ERROR, CRITICAL)"
)


@task(
    default=True,
    help={
        **_update_and_delete_help,
        'threads': (
            "Maximum number of concurrent threads to use in"
            " collecting resources' column information from the data warehouse"
            " and in creating/updating the corresponding property files. Each"
            " thread will run dbt's get_columns_in_query macro against the"
            " data warehouse."
        ),
    },
    auto_shortflags=False,
)
def update(
    ctx,
    resource_type=None,
    select=None,
    models=None,
    exclude=None,
    selector=None,
    project_dir=None,
    profiles_dir=None,
    profile=None,
    target=None,
    vars=None,
    bypass_cache=None,
    state=None,
    log_level=None,
    threads=1,
):
    """
    Update property file(s) for the specified set of resources

    :param ctx: An Invoke context object
    :param resource_type: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param select: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param models: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param exclude: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param selector: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param project_dir: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param profiles_dir: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param profile: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param target: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param vars: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param bypass_cache: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param state: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param log_level: One of Python's standard logging levels
        (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    :param threads: Maximum number of concurrent threads to use in
        collecting resources' column information from the data warehouse
        and in creating/updating the corresponding property files. Each
        thread will run dbt's get_columns_in_query macro against the
        data warehouse.
    :return: None
    """
    common_dbt_kwargs, transformed_ls_results = _initiate_alterations(
        ctx,
        resource_type=resource_type,
        select=select,
        models=models,
        exclude=exclude,
        selector=selector,
        project_dir=project_dir,
        profiles_dir=profiles_dir,
        profile=profile,
        target=target,
        vars=vars,
        bypass_cache=bypass_cache,
        state=state,
        log_level=log_level,
    )
    _create_all_property_files(
        ctx,
        transformed_ls_results,
        threads=threads,
        **common_dbt_kwargs,
    )
    print(
        "\n"
        "\n"
        "\n"
        "To add the documentation to fields, use the following prompt for cursor AI \n"
        "after passing the md file and the sql file of the model as context: \n"
        "\033[94m"  # Start blue color
        "Check the query for the sql model of this md file, and fill out the empty doc snippets\n"
        " (only the empty doc snippets, do not edit the ones that already have content).\n"
        "For each field the info should include:\n"
        " - description\n"
        " - column level lineage\n"
        " - calculation or field logic (if the field is derived)\n"
        "Check this file well, it is really important that the info is correct\n"
        "Give me the change immediately, do not wait for me to ask several times to change the file\n"
        "\033[0m"  # Reset color
        "\n"
    )


@task(
    help=_update_and_delete_help,
    auto_shortflags=False,
)
def delete(
    ctx,
    resource_type=None,
    select=None,
    models=None,
    exclude=None,
    selector=None,
    project_dir=None,
    profiles_dir=None,
    profile=None,
    target=None,
    vars=None,
    bypass_cache=None,
    state=None,
    log_level=None,
):
    """
    Delete property file(s) for the specified set of resources

    :param ctx: An Invoke context object
    :param resource_type: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param select: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param models: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param exclude: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param selector: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param project_dir: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param profiles_dir: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param profile: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param target: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param vars: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param bypass_cache: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param state: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param log_level: One of Python's standard logging levels
        (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    :return: None
    """
    _, transformed_ls_results = _initiate_alterations(
        ctx,
        resource_type=resource_type,
        select=select,
        models=models,
        exclude=exclude,
        selector=selector,
        project_dir=project_dir,
        profiles_dir=profiles_dir,
        profile=profile,
        target=target,
        vars=vars,
        bypass_cache=bypass_cache,
        state=state,
        log_level=log_level,
    )
    _delete_all_property_files(ctx, transformed_ls_results)


@task
def echo_macro(ctx):
    """
    Print out the configured macro for the user to
    copy to their dbt project

    :param ctx: An Invoke context object
    :return: None
    """
    _LOGGER.info(
        f'Copy and paste the following macro into your dbt project:'
        f'\n{_utils.get_macro(_MACRO_NAME)}'
    )


def _read_manifest(target_path):
    """
    Read the dbt manifest file into something we can parse
    :param target_path: The dbt target_path for the project
        https://docs.getdbt.com/reference/project-configs/target-path
    :return: dict representing a dbt manifest
        https://docs.getdbt.com/reference/artifacts/manifest-json
    """
    with open(
        Path(target_path, 'manifest').with_suffix('.json'),
        "r",
        encoding='utf-8',
    ) as manifest_json:
        return json.loads(manifest_json.read())


@task(
    help={
        **_update_and_delete_help,
    },
    auto_shortflags=False,
)
def migrate(
    ctx,
    resource_type=None,
    select=None,
    models=None,
    exclude=None,
    selector=None,
    project_dir=None,
    profiles_dir=None,
    profile=None,
    target=None,
    vars=None,
    bypass_cache=None,
    state=None,
    log_level=None,
):
    """
    Change structure from multiple resources per property file to one
    resource per property file

    :param ctx: An Invoke context object
    :param resource_type: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param select: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param models: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param exclude: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param selector: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param project_dir: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param profiles_dir: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param profile: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param target: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param vars: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param bypass_cache: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param state: An argument for listing dbt resources
        (run "dbt ls --help" for details)
    :param log_level: One of Python's standard logging levels
        (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    :return: None
    """
    _, transformed_ls_results = _initiate_alterations(
        ctx,
        resource_type=resource_type,
        select=select,
        models=models,
        exclude=exclude,
        selector=selector,
        project_dir=project_dir,
        profiles_dir=profiles_dir,
        profile=profile,
        target=target,
        vars=vars,
        bypass_cache=bypass_cache,
        state=state,
        log_level=log_level,
    )
    
    nodes = _read_manifest(ctx['target_path'])['nodes']
    migration_map = defaultdict(list)
    
    for node, metadata in nodes.items():
        if metadata['original_file_path'] not in transformed_ls_results:
            continue
        elif not metadata.get('patch_path'):
            continue
            
        existing_property_path = Path(
            ctx.config['project_path'],
            metadata.get('patch_path').split('//')[-1],
        ).with_suffix('.md')  # Change to .md extension
        
        resource_path = Path(
            ctx.config['project_path'],
            metadata['original_file_path'],
        )
        
        migration_map[existing_property_path].append(
            {
                'name': metadata['name'],
                'resource_type': metadata['resource_type'],
                'resource_type_plural': _SUPPORTED_RESOURCE_TYPES.get(
                    metadata['resource_type']
                ),
                'resource_path': resource_path,
                'property_path': resource_path.with_suffix('.md'),
            }
        )

    # Loop through the migration_map to perform the migration
    for existing_property_path, resource_list in migration_map.items():
        # Read existing markdown content if it exists
        existing_content = _read_md_file(existing_property_path) if existing_property_path.exists() else ""
        
        for resource in resource_list:
            # Skip if the properties are already in the correct location
            if existing_property_path == resource['property_path']:
                continue
                
            _LOGGER.info(
                f"""Moving "{resource['name']}" definition from"""
                f""" {str(existing_property_path.resolve())} to"""
                f""" {str(resource['property_path'].resolve())}"""
            )
            
            try:
                # Extract resource information from existing content if possible
                resource_content = _extract_resource_content(existing_content, resource['name'])
                
                if not resource_content:
                    # If no existing content found, create new markdown structure
                    columns = _get_columns(
                        ctx,
                        str(resource['resource_path']),
                        {'name': resource['name'], 'resource_type': resource['resource_type'], 'config': {'materialized': 'table'}},
                    )
                    resource_content = _structure_md_content(resource, columns or [])
                
                # Write the new markdown file
                _write_md_file(resource['property_path'], resource_content)
                _LOGGER.info(f"Created {resource['property_path']}")
                
            except Exception:
                _LOGGER.exception(
                    f"Failed to create {resource['property_path']}"
                )

        # Remove the original file if it's empty or only contains headers
        if existing_property_path.exists():
            remaining_content = _read_md_file(existing_property_path)
            if not remaining_content or remaining_content.strip() == "":
                try:
                    existing_property_path.unlink()
                    _LOGGER.info(
                        f"Deleted {str(existing_property_path.resolve())}"
                    )
                except Exception:
                    _LOGGER.exception(
                        f"Failed to delete {str(existing_property_path.resolve())}"
                    )


def _extract_resource_content(content, resource_name):
    """
    Extract content for a specific resource from markdown content
    
    :param content: Full markdown content
    :param resource_name: Name of the resource to extract
    :return: Extracted content for the resource or None if not found
    """
    if not content:
        return None
        
    # Find the section for this resource
    pattern = rf"#\s*{re.escape(resource_name)}.*?(?=\n#\s*[^#]|\Z)"
    match = re.search(pattern, content, re.DOTALL)
    
    if match:
        return match.group(0).strip()
    return None


def _initiate_alterations(ctx, **kwargs):
    """
    Retrieve the dbt keyword arguments that are common to multiple dbt
    commands as well as the transformed results of the "dbt ls" command

    :param ctx: An Invoke context object
    :param kwargs:
    :return: A 2-tuple of:
        1. The dbt keyword arguments that are common to multiple dbt
        commands
        2. The transformed results of the "dbt ls" command
    """
    if kwargs.get('log_level'):
        _LOGGER.setLevel(kwargs.get('log_level').upper())
    resource_type = kwargs.get('resource_type')
    _assert_supported_resource_type(resource_type)
    project_dir = kwargs.get('project_dir')
    _utils.get_project_info(ctx, project_dir=project_dir)
    common_dbt_kwargs = {
        'project_dir': project_dir or ctx.config['project_path'],
        'profiles_dir': kwargs.get('profiles_dir'),
        'profile': kwargs.get('profile'),
        'target': kwargs.get('target'),
        'vars': kwargs.get('vars'),
        'bypass_cache': kwargs.get('bypass_cache'),
    }
    # Get the paths and resource types of the
    # resources for which to create property files
    transformed_ls_results = _transform_ls_results(
        ctx,
        resource_type=resource_type,
        select=kwargs.get('select'),
        models=kwargs.get('models'),
        exclude=kwargs.get('exclude'),
        selector=kwargs.get('selector'),
        state=kwargs.get('state'),
        **common_dbt_kwargs,
    )
    return common_dbt_kwargs, transformed_ls_results


def _transform_ls_results(ctx, **kwargs):
    """
    Run the "dbt ls" command to select resources and determine their
    resource type and path. Then filter out unsupported resource types
    and return the results in a dictionary.

    :param ctx: An Invoke context object
    :param kwargs: Arguments for listing dbt resources
        (run "dbt ls --help" for details)
    :return: Dictionary where the key is the resource path
        and the value is dictionary form of the resource's json
    """
    # Run dbt ls to retrieve resource path and json information
    _LOGGER.info('Searching for matching resources...')
    potential_results = _utils.dbt_ls(
        ctx,
        supported_resource_types=_SUPPORTED_RESOURCE_TYPES,
        logger=_LOGGER,
        output='json',
        **kwargs,
    )
    potential_result_paths = None
    results = dict()
    for i, potential_result in enumerate(potential_results):
        potential_result_path = potential_result['original_file_path']
        if Path(ctx.config['project_path'], potential_result_path).exists():
            results[potential_result_path] = potential_result
    _LOGGER.info(
        f"Found {len(results)} matching resources in dbt project"
        f' "{ctx.config["project_name"]}"'
    )
    if _LOGGER.level <= 10:
        for resource in results:
            _LOGGER.debug(resource)
    return results


def _create_all_property_files(
    ctx,
    transformed_ls_results,
    threads=1,
    **kwargs,
):
    """
    For each resource from dbt ls, create or update a property file

    :param ctx: An Invoke context object
    :param transformed_ls_results: Dictionary where the key is the
        resource path and the value is the dictionary form of the
        resource's json
    :param threads: Maximum number of concurrent threads to use in
        collecting resources' column information from the data warehouse
        and in creating/updating the corresponding property files. Each
        thread will run dbt's get_columns_in_query macro against the
        data warehouse.
    :param kwargs: Additional arguments for _utils.dbt_run_operation
        (run "dbt run-operation --help" for details)
    :return: None
    """
    # Run a check that will fail if the _MACRO_NAME macro does not exist
    if not _utils.macro_exists(ctx, _MACRO_NAME, logger=_LOGGER, **kwargs):
        _utils.add_macro(ctx, _MACRO_NAME, logger=_LOGGER)
    # Handle the creation of property files in separate threads
    transformed_ls_results_length = len(transformed_ls_results)
    with ThreadPoolExecutor(max_workers=threads) as executor:
        futures = {
            executor.submit(
                _create_property_file,
                ctx,
                k,
                v,
                i + 1,
                transformed_ls_results_length,
                **kwargs,
            ): {'index': i + 1, 'resource_location': k}
            for i, (k, v) in enumerate(transformed_ls_results.items())
        }
        # Log success or failure for each thread
        successes = 0
        failures = 0
        for future in as_completed(futures):
            index = futures[future]['index']
            resource_location = futures[future]['resource_location']
            progress = (
                f'Resource {index} of {transformed_ls_results_length},'
                f' {resource_location}'
            )
            if future.exception() is not None:
                _LOGGER.error(f'{"[FAILURE]":>{_PROGRESS_PADDING}} {progress}')
                failures += 1
                # Store exception message for later when all tracebacks
                # for failed futures will be logged
                e = future.exception()
                exception_lines = traceback.format_exception(
                    type(e), e, e.__traceback__
                )
                futures[future][
                    'exception_message'
                ] = f'{progress}\n{"".join(exception_lines)}'
            else:
                _LOGGER.info(f'{"[SUCCESS]":>{_PROGRESS_PADDING}} {progress}')
                successes += 1
    # Log traceback for all failures at the end
    if failures:
        exception_messages = list()
        # Looping through futures instead of as_completed(futures) so
        # that the failed futures are displayed in order of submission,
        # rather than completion
        for future in futures:
            exception_message = futures[future].get('exception_message')
            if exception_message:
                exception_messages.append(exception_message)
        if exception_messages:
            exception_messages = '\n'.join(exception_messages)
            _LOGGER.error(
                f'Tracebacks for all failures:\n\n{exception_messages}'
            )
    # Log result summary
    _LOGGER.info(
        f'{"[DONE]":>{_PROGRESS_PADDING}}'
        f' Total: {successes + failures},'
        f' Successes: {successes},'
        f' Failures: {failures}'
    )


def _delete_all_property_files(ctx, transformed_ls_results):
    """
    Delete property files in markdown format

    :param ctx: An Invoke context object
    :param transformed_ls_results: Dictionary where the key is the resource path
    :return: None
    """
    resource_paths = [
        Path(ctx.config['project_path'], resource_location)
        for resource_location in transformed_ls_results
    ]
    property_paths = [
        rp.with_suffix('.md')
        for rp in resource_paths
        if rp.with_suffix('.md').exists()
    ]
    _LOGGER.info(
        f'{len(property_paths)} of {len(resource_paths)}'
        f' have existing property files'
    )
    
    if len(property_paths) > 0:
        deletion_message_md_paths = '\n'.join(
            [str(property_path) for property_path in property_paths]
        )
        deletion_message_prefix = '\nThe following files will be deleted:\n\n'
        deletion_message_suffix = (
            f'\n\nAre you sure you want to delete these'
            f' {len(property_paths)} file(s) (answer: y/n)?\n'
        )
        deletion_confirmation = input(
            f'{deletion_message_prefix}'
            f'{deletion_message_md_paths}'
            f'{deletion_message_suffix}'
        )
        
        while deletion_confirmation.lower() not in ['y', 'n']:
            deletion_confirmation = input(
                '\nPlease enter "y" to confirm deletion'
                ' or "n" to abort deletion.\n'
            )
        
        if deletion_confirmation.lower() == 'y':
            for file in property_paths:
                os.remove(file)
            _LOGGER.info('Deletion confirmed.')
        else:
            _LOGGER.info('Deletion aborted.')
    else:
        _LOGGER.info('There are no files to delete.')


def _create_property_file(
    ctx,
    resource_location,
    resource_dict,
    counter,
    total,
    **kwargs,
):
    """
    Create a property file in markdown format

    :param ctx: An Invoke context object
    :param resource_location: The location of the file representing the resource
    :param resource_dict: A dictionary representing the json output for this resource
    :param counter: An integer assigned to this file
    :param total: An integer representing the total number of files to be created
    :param kwargs: Additional arguments for _utils.dbt_run_operation
    :return: None
    """
    _LOGGER.info(
        f'{"[START]":>{_PROGRESS_PADDING}}'
        f' Resource {counter} of {total},'
        f' {resource_location}'
    )
    columns = _get_columns(ctx, resource_location, resource_dict, **kwargs)
    property_path = Path(
        ctx.config['project_path'], resource_location
    ).with_suffix('.md')
    
    # Check if file exists and read content
    existing_content = _read_md_file(property_path) if property_path.exists() else None
    
    # If file doesn't exist or is empty, create new content
    if not existing_content:
        content = _structure_md_content(resource_dict, columns)
    else:
        # Update existing content with new columns
        new_content = []
        for column in columns:
            if not _check_field_exists(existing_content, column, resource_dict['name']):
                new_content.extend([
                    "",  # One blank line before docs
                    f"{{% docs {resource_dict['name']}__{column} %}}",
                    "",
                    "{% enddocs %}",
                    ""  # One blank line after docs
                ])
        content = existing_content.rstrip() + "\n" + "\n".join(new_content) if new_content else existing_content
    
    _write_md_file(property_path, content)


def _get_columns(ctx, resource_location, resource_dict, **kwargs):
    """
    Get a list of the column names in a resource

    :param ctx: An Invoke context object
    :param resource_location: The location of the file representing the
        resource
    :param resource_dict: A dictionary representing the json output for
        this resource from the "dbt ls" command
    :param kwargs: Additional arguments for _utils.dbt_run_operation
        (run "dbt run-operation --help" for details)
    :return: A list of the column names in the resource
    """
    resource_path = Path(resource_location)
    materialized = resource_dict['config']['materialized']
    resource_type = resource_dict['resource_type']
    resource_name = resource_dict['name']
    if materialized != 'ephemeral' and resource_type != 'analysis':
        result_lines = _utils.dbt_run_operation(
            ctx,
            _MACRO_NAME,
            hide=True,
            logger=_LOGGER,
            resource_name=resource_name,
            **kwargs,
        )
    # Ephemeral and analysis resource types are not materialized in the
    # data warehouse, so the compiled versions of their SQL statements
    # are used instead
    else:
        resource_path = Path(ctx.config['compiled_path'], resource_path)
        with open(resource_path, 'r') as f:
            lines = f.readlines()
        # Get and clean the SQL code
        lines = [line.strip() for line in lines if line.strip()]
        sql = "\n".join(lines)
        result_lines = _utils.dbt_run_operation(
            ctx, _MACRO_NAME, hide=True, logger=_LOGGER, sql=sql, **kwargs
        )

    relevant_lines = list(
        filter(
            lambda x: x["info"].get("code") == "I062",
            result_lines,
        )
    )
    if len(relevant_lines) >= 1:
        relevant_line = relevant_lines[-1]
        columns = relevant_line.get(
            'msg',
            relevant_line.get('info', dict()).get('msg'),
        )
        # In some version of dbt columns are not passed as valid json but as
        # a string representation of a list
        is_string_list = (
            isinstance(columns, str)
            and columns.startswith('[')
            and columns.endswith(']')
        )
        if is_string_list:
            columns = ast.literal_eval(columns)
        return columns


def _structure_md_content(resource_dict, columns_list):
    """
    Structure markdown content for a resource with docs templates
    
    :param resource_dict: Dictionary containing resource information
    :param columns_list: List of columns
    :return: Formatted markdown content
    """
    resource_name = resource_dict['name']

    content = [""]

    for column in columns_list:
        content.extend([
            "",  # One blank line before docs
            f"{{% docs {resource_name}__{column} %}}",
            "",
            "{% enddocs %}",
            ""  # One blank line after docs
        ])
    
    return "\n".join(content)


def _assert_supported_resource_type(resource_type):
    """
    Assert that the given resource type is in the list of supported
        resource types

    :param resource_type: A dbt resource type
    :return: None
    """
    try:
        assert (
            resource_type is None
            or resource_type.lower() in _SUPPORTED_RESOURCE_TYPES
        )
    except AssertionError:
        msg = (
            f'Sorry, this tool only supports the following resource types:'
            f' {list(_SUPPORTED_RESOURCE_TYPES.keys())}'
        )
        _LOGGER.exception(msg)
        raise


def _read_md_file(file_path):
    """
    Read a markdown file and parse its content
    
    :param file_path: Path to the markdown file
    :return: Dictionary containing the parsed content
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        return content
    except FileNotFoundError:
        return None


def _write_md_file(file_path, content):
    """
    Write content to a markdown file
    
    :param file_path: Path to the markdown file
    :param content: Content to write
    """
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)


def _check_field_exists(content, field_name, resource_name):
    """
    Check if a field exists in the markdown content
    
    :param content: Markdown content
    :param field_name: Name of the field to check
    :param resource_name: Name of the resource
    :return: Boolean indicating if field exists
    """
    pattern = rf"{{% docs {re.escape(resource_name)}__{re.escape(field_name)} %}}"
    return bool(re.search(pattern, content)) if content else False




