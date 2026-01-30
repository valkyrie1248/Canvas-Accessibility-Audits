import marimo

__generated_with = "0.19.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    import os
    import sys
    import time
    import tomllib
    from datetime import datetime
    from pathlib import Path
    from typing import TYPE_CHECKING

    import pandas as pd
    import requests
    from bs4 import BeautifulSoup
    from canvasapi import Canvas
    from dateutil import tz
    from dotenv import load_dotenv
    from loguru import logger
    from playwright.sync_api import sync_playwright

    if TYPE_CHECKING:
        from canvasapi import Course, File, PaginatedList
        from pandas.core.frame import DataFrame
    return DataFrame, Path, datetime, load_dotenv, logger, os, pd, sys, tz


@app.cell
def _(Path, datetime, load_dotenv, logger, os, pd, sys, tz):
    # =============================================================================
    # CONFIGURATION - General/Display
    # =============================================================================

    pd.options.display.float_format = lambda x: f"{x:.3f}"
    pd.options.display.max_rows = 500
    pd.options.display.max_columns = 100
    pd.set_option("display.precision", 4)


    # =============================================================================
    # CONFIGURATION - Logger
    # =============================================================================
    logger.remove()
    _stderr_logger = logger.add(
        sys.stderr,
        colorize=True,
        format="<green>{time:YYYY-MM-DD at HH:mm:ss}</green>| <level>{level}</level> | <level>{message}</level> | {extra}",
        backtrace=True,
        diagnose=True,
        level="TRACE",
    )
    _file_logger = logger.add(
        "accessibility_checklist_generator.log",
        colorize=True,
        rotation="1 week",
        retention=5,
        format="<green>{time:YYYY-MM-DD at HH:mm:ss}</green>| <level>{level}</level> | <level>{message}</level> | {extra}",
        backtrace=True,
        diagnose=True,
        level="TRACE",
    )
    # =============================================================================
    # CONFIGURATION - Constants
    # =============================================================================
    PROJECT_ROOT = Path(__file__).parent.parent
    CONFIG_FILE = PROJECT_ROOT / "config.toml"
    RUN_ID: int = int(datetime.now(tz=tz.tzlocal()).timestamp())

    # Import Credentials from .env
    _ = load_dotenv()

    # Canvas Credentials
    CANVAS_TOKEN: str | None = os.environ.get("CANVAS_TOKEN")

    # Ally Credentials
    ALLY_KEY: str | None = os.environ.get("KEY")
    ALLY_SECRET: str | None = os.environ.get("SECRET")
    return (CONFIG_FILE,)


@app.cell
def _():
    # Import functions from other file
    from accessibility_checklist_prototype import (
        load_config,
        initialize_canvas_course,
        fetch_course_content,
        parse_course_file_data,
        extract_html,
        parse_html_content,
        get_ally_session_cookie,
        trigger_ally_export,
        download_s3_file,
        get_ally_report,
        create_canvas_data_df,
        create_ally_df,
        clean_ally_df,
        join_data_sources,
        save_as_csv,
        main,
    )
    return (
        extract_html,
        fetch_course_content,
        initialize_canvas_course,
        load_config,
        parse_course_file_data,
        parse_html_content,
    )


@app.cell
def _(
    CONFIG_FILE,
    fetch_course_content,
    initialize_canvas_course,
    load_config,
    logger,
):
    """Orchestrate the retrieval and combining of Ally and Canvas data."""

    logger.info("Starting program...")
    config = load_config(CONFIG_FILE)
    course_id = config.get("course_id")

    course_obj = initialize_canvas_course(config, course_id)

    course_content_dict = fetch_course_content(course_obj, config)
    logger.info(course_content_dict)
    course_files = course_content_dict.get("Files")
    return config, course_content_dict, course_files, course_id


@app.cell
def _(
    config,
    course_content_dict,
    course_files,
    course_id,
    extract_html,
    parse_course_file_data,
    parse_html_content,
):
    course_file_data = parse_course_file_data(course_files, course_id)
    potential_a11y_issues = []
    for content_type, course_content in course_content_dict.items():
        if content_type != "Files":
            html_string, content_name = extract_html(
                course_content, content_type, config
            )
            content_type_a11y_issues = parse_html_content(
                html_string,
                course_id,
                content_type,
                content_name,
                "URL_PLACEHOLDER",
            )
            potential_a11y_issues.extend(content_type_a11y_issues)
    potential_a11y_issues
    return course_file_data, potential_a11y_issues


@app.cell
def _(
    DataFrame,
    course_file_data,
    dtypebackend,
    logger,
    pd,
    potential_a11y_issues,
):
    potential_a11y_issues.extend(course_file_data)
    potential_a11y_issues
    logger.info("Initializing DataFrame")
    canvas_df: DataFrame = pd.DataFrame(potential_a11y_issues).convert_dtypes(
        dtype_backend=dtypebackend
    )
    canvas_df
    return


@app.cell
def _(logger, pd):
    file_path = "/Users/harkmorper/Desktop/canvas_a11y_audits/data/ally_44160_1768963012.csv"
    dtypebackend = "pyarrow"

    try:
        ally_df = pd.read_csv(
            file_path,
            dtype_backend=dtypebackend,
            usecols=None,
            engine="pyarrow",
        ).rename(columns={"Name": "display_name"})
    except Exception as e:
        logger.critical(f"Pandas load failed: {e}")
        raise
    ally_df = ally_df.loc[ally_df["Deleted at"].isna()]
    print(ally_df.shape)
    ally_df
    return ally_df, dtypebackend


@app.cell
def _(ally_df, config, logger):
    flags = config.get("ally").get("flag_columns")
    if not flags:
        error_message = "No flag columns found in the configuration."
        raise ValueError(error_message)

    flagged_df = ally_df[flags]
    flagged_df = flagged_df[flagged_df == 1]
    flag_list = (
        flagged_df.reset_index()
        .melt(id_vars="index", value_vars=flags)
        .dropna()
        .groupby("index")["variable"]
        .agg(", ".join)
    )
    ally_df["Flags"] = ally_df.index.map(flag_list)
    cleaned_ally_df = ally_df.drop(columns=flags)
    logger.debug(f"shape of cleeaned_ally_df = {cleaned_ally_df.shape}")
    cleaned_ally_df
    return


@app.cell
def _(ally_df):
    ally_df
    return


if __name__ == "__main__":
    app.run()
