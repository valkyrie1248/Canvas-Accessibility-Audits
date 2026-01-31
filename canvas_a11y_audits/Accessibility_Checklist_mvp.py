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
    return Path, datetime, load_dotenv, logger, os, pd, sys, tz


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
        fetch_course_content,
        initialize_canvas_course,
        load_config,
        parse_course_file_data,
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
def _(course_content_dict):
    course_content_dict["Pages"][1]
    return


@app.cell
def _(course_files, course_id, parse_course_file_data):
    course_file_data = parse_course_file_data(course_files, course_id)
    return


@app.cell
def _(config, course_content_dict):
    potential_a11y_issues = []
    getattr(course_content_dict["Pages"][0],"body")
    type_config = config["content_types"].get("Pages")
    test = type_config["html_field"]
    getattr(course_content_dict["Pages"][0],test)

    for item in course_content_dict["Pages"]:
        print(item.__dict__)
        print(getattr(item,test))

    #     logger.info(f"Extracting html_data for {content_type}")
    #     type_config = config_dict["content_types"].get(content_type)
    #     if not type_config:
    #         error_message = f"Unknown content type: {content_type}"
    #         raise ValueError(error_message)

    #     html_string = getattr(course_content, type_config["html_field"], "")
    #     content_name = getattr(
    #         course_content,
    #         type_config["title_field"],
    #         "Untitled",
    #     )
    #     return html_string, content_name


    # for content_type, course_content in course_content_dict.items():
    #     if content_type != "Files":
    #         html_string, content_name = extract_html(
    #             course_content, content_type, config
    #         )
    #         content_type_a11y_issues = parse_html_content(
    #             html_string,
    #             course_id,
    #             content_type,
    #             content_name,
    #             "URL_PLACEHOLDER",
    #         )
    #         potential_a11y_issues.extend(content_type_a11y_issues)
    # potential_a11y_issues
    return


if __name__ == "__main__":
    app.run()
