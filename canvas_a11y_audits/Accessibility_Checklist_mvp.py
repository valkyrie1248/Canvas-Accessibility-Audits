import marimo

__generated_with = "0.19.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    import sys
    import tomllib
    import datetime
    from pathlib import Path
    from typing import TYPE_CHECKING

    import pandas as pd
    from canvasapi import Canvas
    from bs4 import BeautifulSoup
    from loguru import logger
    from pandas.core.frame import DataFrame

    if TYPE_CHECKING:
        from canvasapi.course import Course
        from canvasapi.file import File
        from canvasapi.paginated_list import PaginatedList
        from pandas.core.frame import DataFrame
    return Path, datetime, logger, mo, pd, sys


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
    create_canvas_data_df,
    trigger_ally_export,
    download_s3_file,
    get_ally_report,
    create_ally_df,
    clean_ally_df,
    join_data_sources,
    create_csv,
    main
    )
    return (
        clean_ally_df,
        create_ally_df,
        create_canvas_data_df,
        extract_html,
        fetch_course_content,
        initialize_canvas_course,
        join_data_sources,
        load_config,
        parse_course_file_data,
        parse_html_content,
    )


@app.cell
def _(config):
    config.get("ally").get("flag_columns")
    return


@app.cell
def _(
    config_file,
    datetime,
    fetch_course_content,
    initialize_canvas_course,
    load_config,
    logger,
    parse_course_file_data,
):
    """Orchestrate the retrieval and combining of Ally and Canvas data."""
    logger.info("Starting program...")
    run_id = int(datetime.datetime.now().timestamp())
    config = load_config(config_file)
    course_id = config.get("course_id")

    course_obj = initialize_canvas_course(config)

    course_content_dict = fetch_course_content(course_obj, config)

    course_files = course_content_dict.get("Files")
    course_file_data = parse_course_file_data(course_files,course_id)
    return config, course_content_dict, course_file_data, course_id


@app.cell
def _(
    config,
    course_content_dict,
    course_id,
    extract_html,
    parse_html_content,
):
    potential_a11y_issues = []
    for content_type,course_content in course_content_dict.items():
        if content_type != "Files":
            html_string,content_name = extract_html(course_content,content_type,config)
            content_type_a11y_issues = parse_html_content(html_string,course_id,content_type,content_name,"URL_PLACEHOLDER")
            potential_a11y_issues.extend(content_type_a11y_issues)
    return (potential_a11y_issues,)


@app.cell
def _(
    clean_ally_df,
    config,
    course_file_data,
    create_ally_df,
    create_canvas_data_df,
    join_data_sources,
    potential_a11y_issues,
):
    canvas_df = create_canvas_data_df(config,course_file_data,potential_a11y_issues)
    ally_df = create_ally_df(
        "/Users/harkmorper/Downloads/ally-43754-Fa25_-_COUN_549_-_Motivational_Interviewing-2026-01-15-17-55.csv",
    ).pipe(clean_ally_df,config)
    drop_joint_columns = config.get("joint").get("drop_columns")
    joint_df = join_data_sources(canvas_df, ally_df, drop_joint_columns)
    joint_df
    return (joint_df,)


@app.cell
def _(joint_df):
    joint_df.nunique()
    return


@app.cell
def _(Path, logger, pd, sys):
    # =============================================================================
    # CONFIGURATION
    # =============================================================================

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
    # CONFIGURATION - 
    # =============================================================================
    PROJECT_ROOT = Path(__file__).parent.parent
    config_file = PROJECT_ROOT / "config.toml"
    return (config_file,)


@app.cell
def _():
    # pages = course.get_pages()
    # for page in pages:
    #     print(page.__dict__)
    # for item in pages[0].__dict__:
    #     print(item)
    # pages[0].__dict__.get("hide_from_students")
    return


@app.cell
def _(mo):
    mo.md(r"""
    <!-- for fi in files[0].__dict__:
        print(fi)
    print("============================")
    for p in pages[0].__dict__:
        print(p) -->
    """)
    return


@app.cell
def _():
    # # canvas_df_pdf = canvas_df.loc[canvas_df["content-type"].str.contains("pdf")]
    # # ally_df_pdf = ally_df.loc[ally_df["Mime type"].str.contains("pdf")]
    # # for l_ally in list(ally_df_pdf["Name"].sort_values()):
    # #     if l_ally not in list(canvas_df_pdf["display_name"].sort_values()):
    # #         print(l_ally)
    # # for l_canvas in list(canvas_df_pdf["display_name"].sort_values()):
    # #     if l_canvas not in list(ally_df_pdf["Name"].sort_values()):
    # #         print(l_canvas)

    # canvas_df_xml = canvas_df.loc[canvas_df["content-type"].str.contains("xml")]
    # ally_df_xml = ally_df.loc[ally_df["Mime type"].str.contains("xml")]
    # for l_ally in list(ally_df_xml["Name"].sort_values()):
    #     if l_ally not in list(canvas_df_xml["display_name"].sort_values()):
    #         print(l_ally)
    # for l_canvas in list(canvas_df_xml["display_name"].sort_values()):
    #     if l_canvas not in list(ally_df_xml["Name"].sort_values()):
    #         print(l_canvas)
    return


@app.cell
def _(config, logger):
    from playwright.sync_api import sync_playwright

    logger.catch("There was an error getting the session cookie: ")
    def get_ally_session_cookie(config: dict) -> str:
        """
        Logs into the Ally Institutional Report via LTI Key/Secret and retrieves the session cookie.
        """
        # 1. Retrieve credentials
        ally_config = config.get("ally", {})
        key = ally_config.get("key")
        secret = ally_config.get("secret")
    
        if not key or not secret:
            raise ValueError("Ally Key and Secret must be present in the config.")

        target_url = "https://prod.ally.ac/report/11637"

        with sync_playwright() as p:
            # Launch browser
            browser = p.chromium.launch(headless=False)
            context = browser.new_context()
            page = context.new_page()

            # 2. Navigate
            page.goto(target_url)

            # 3. Fill Credentials (Selectors based on your HTML)
            # HTML: <input ... id="key">
            page.fill('#key', key)
        
            # HTML: <input ... id="secret">
            page.fill('#secret', secret)

            # 4. Submit
            # HTML: <button type="submit" ...>Sign in</button>
            page.click('button[type="submit"]')
        
            # Wait for the report to load (Angular app)
            page.wait_for_load_state("networkidle")

            # 5. Extract Cookie
            cookies = context.cookies()
            ally_cookie = next(
                (c for c in cookies if "ally.ac" in c["domain"] and "session" in c["name"]), 
                None
            )

            browser.close()

            if ally_cookie:
                return f"{ally_cookie['name']}={ally_cookie['value']}"
            else:
                raise ValueError("Failed to retrieve Ally session cookie. Check Key/Secret credentials.")

    get_ally_session_cookie(config)
    return


if __name__ == "__main__":
    app.run()
