import marimo

__generated_with = "0.19.2"
app = marimo.App(width="columns")

with app.setup:
    # Initialization code that runs before allimport marimo as mo
    import marimo as mo
    import os
    import sys
    import time
    import tomllib
    import re
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

    # =============================================================================
    # FUNCTIONS - Import Functions from Local Module
    # =============================================================================

    from accessibility_checklist_prototype import (
        load_config,
        initialize_canvas_course,
        fetch_course_content,
        parse_course_file_data,
        extract_html,
        parse_html_content,
    )

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
        level="DEBUG",
    )
    _file_logger = logger.add(
        "accessibility_checklist_generator.log",
        colorize=True,
        rotation="1 week",
        retention=5,
        format="<green>{time:YYYY-MM-DD at HH:mm:ss}</green>| <level>{level}</level> | <level>{message}</level> | {extra}",
        backtrace=True,
        diagnose=True,
        level="INFO",
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


@app.cell
def _():
    # Initialize configuration settings, course object, course content dictionary, & course files object
    # These are the base things I need created in order to start experimenting with refactoring parse_html_content
    config = load_config(CONFIG_FILE)
    course_id = config.get("course_id")

    course_obj = initialize_canvas_course(config, course_id)
    logger.info(course_obj)
    course_content_dict = fetch_course_content(course_obj, config)
    logger.info(course_content_dict)
    course_files = course_content_dict.get("Files")
    return course_files, course_id, course_obj


@app.cell(column=1, hide_code=True)
def _():
    mo.md(r"""
    # Functions I'm Refactoring

    ```
    @logger.catch()
    def parse_course_file_data(
        course_files: PaginatedList[File],
        course_id: str,
    ) -> list[dict[str, str]]:
        "\"\"Extract and organize important data about course files.

        Parameters
        ----------
        course_files: PaginatedList[File]
            PaginatedList of File objects assembled by CanvasAPI library.
        course_id: str
            Unique Canvas identifier for the course you are reviewing.

        Returns
        -------
        course_file_data: list[dict[str,str|int]]
            List of dictionaries, with each dictionary corresponding to a single
            file in the course site that may have accessibility issues.
        "\"\"
        logger.info("Fetching course files data...")

        course_file_data = [
            {
                "course_id": course_id,
                "audit_status": "New Content",
                "content-type": "File",
                "display_name": file.__dict__.get("display_name"),
                "url": file.__dict__.get("url"),
                "reason_extracted": file.__dict__.get("content-type"),
                "canvas_flags": "See Ally",
                "canvas_details": "See Ally",
                "alt_text": "N/A",
                "run_id": RUN_ID,
            }
            for file in course_files
        ]
        logger.debug(len(course_file_data))
        for i, item in enumerate(course_file_data):
            logger.debug(f"Index {i}: {item}")
        return course_file_data


    @logger.catch()
    def extract_html(course_content, content_type: str, config_dict: dict) -> str:
        "\"\"
        Extract HTML data from course content object for scraping.

        Returns the raw HTML string.

        Parameters
        ----------
        course_content:
            Packaged data from the course site returned by fetch_course_content
        content_type: str
            String alias for the content type you want to fetch. Acceptable inputs:
            - 'pages'
            - 'assignments'
            - 'discussions'
            - 'quizzes'
        config_dict: dict
            Dictionary containing all rules, equivalencies, and settings

        Returns
        -------
        html_string: str
            Raw HTML data for the content

        Raises
        ------
        ValueError
            If content_type is not one of the types outlined in config file.
        "\"\"
        logger.info(f"Extracting html_data for {content_type}")
        type_config = config_dict["content_types"].get(content_type)
        if not type_config:
            error_message = f"Unknown content type: {content_type}"
            raise ValueError(error_message)
        html_data_dict = {}
        html_attribute = type_config["html_field"]
        for item in course_content:
            html_string = getattr(item, html_attribute)
            content_name = getattr(
                course_content,
                type_config["title_field"],
                "Untitled",
            )
            new_key = content_name
            suffix = 0
            while new_key in html_data_dict:
                new_key = f"{content_name}_suffix_{suffix}"
                suffix += 1
            html_data_dict[new_key] = html_string
        return html_data_dict


    @logger.catch()
    def parse_html_content(
        html_string: str,
        course_id: str,
        content_type: str,
        content_name: str,
        content_url: str,
    ) -> list[dict[str, str]]:
        "\"\"
        Search html_content for items that need to be checked for accessibility.

        Returns a list of dictionaries, with each dictionary corresponding to a
        single item in the course site that may have accessibility issues.

        Parameters
        ----------
        html_string: _type_
            Raw HTML extracted from a course content object.
        course_id: str
            Unique Canvas identifier for the course you are reviewing.
        content_type: str
            Type of content that contains the HTML data.
        content_name: str
            Name of the content containing the HTML data.
        content_url: str
            Direct URL for the content containing the HTML data.

        Returns
        -------
        potential_a11y_issues: list[dict[str,str|int]]
            List of dictionaries, with each dictionary corresponding to a single
            item in the course site that may have accessibility issues.
        "\"\"
        potential_a11y_issues = []

        if not html_string:
            logger.debug("html_string is empty. Returning empty list...")
            return potential_a11y_issues
        logger.debug("html string not empty. Extracting now...")
        soup = BeautifulSoup(html_string, "html.parser")

        for a_tag in soup.find_all("a", href=True):
            link_url = a_tag["href"]
            link_text = a_tag.get_text(strip=True)
            potential_a11y_issues.append(
                {
                    "course_id": course_id,
                    "audit_status": "New Content",
                    "content-type": content_type,
                    "display_name": content_name,
                    "url": link_url,
                    "reason_extracted": "Link",
                    "canvas_flags": None,
                    "canvas_details": f"Link to: {link_url} (Link text: '{link_text}')",
                    "alt_text": "N/A",
                    "run_id": RUN_ID,
                },
            )

        for img_tag in soup.find_all("img", src=True):
            img_src = img_tag.get("src", "\")
            alt_text = img_tag.get("alt", "\")
            if not alt_text:
                issue = "Image Missing Alt Text"
            elif not alt_text.strip():
                issue = "Image With Empty Alt Text"
            else:
                issue = "Check Quality of Alt Text"
                logger.info(
                    f"Alt text found for {content_name}. May want to check quality of alt text.",
                )
            potential_a11y_issues.append(
                {
                    "course_id": course_id,
                    "audit_status": "New Content",
                    "content-type": content_type,
                    "display_name": content_name,
                    "url": content_url,
                    "reason_extracted": "Image",
                    "canvas_flags": issue,
                    "canvas_details": f"Image source: {img_src}",
                    "alt_text": alt_text,
                    "run_id": RUN_ID,
                },
            )

        for iframe in soup.find_all("iframe"):
            potential_a11y_issues.append(
                {
                    "course_id": course_id,
                    "audit_status": "New Content",
                    "content-type": content_type,
                    "display_name": content_name,
                    "url": content_url,
                    "reason_extracted": "Embedded Media (Video)",
                    "canvas_flags": "May require manual caption check",
                    "canvas_details": f"May require manual caption check {iframe['src']}",
                    "alt_text": "N/A",
                    "run_id": RUN_ID,
                },
            )

        for video_tag in soup.find_all("video", src=True):
            potential_a11y_issues.append(
                {
                    "course_id": course_id,
                    "audit_status": "New Content",
                    "content-type": content_type,
                    "display_name": content_name,
                    "url": content_url,
                    "reason_extracted": "Embedded Media (Video)",
                    "canvas_flags": "May require manual caption check",
                    "canvas_details": f"May require manual caption check {video_tag['src']}",
                    "alt_text": "N/A",
                    "run_id": RUN_ID,
                },
            )

        for audio_tag in soup.find_all("audio", src=True):
            potential_a11y_issues.append(
                {
                    "course_id": course_id,
                    "audit_status": "New Content",
                    "content-type": content_type,
                    "display_name": content_name,
                    "url": content_url,
                    "reason_extracted": "Embedded Media (Audio)",
                    "canvas_flags": "May require manual caption check",
                    "canvas_details": f"May require manual transcript check {audio_tag['src']}",
                    "alt_text": "N/A",
                    "run_id": RUN_ID,
                },
            )
        logger.debug(len(potential_a11y_issues))
        return potential_a11y_issues
    ```
    """)
    return


app._unparsable_cell(
    r"""
    type_config = config_dict["content_types"].get(content_type)
        if not type_config:
            error_message = f"Unknown content type: {content_type}"
            raise ValueError(error_message)
        html_data_dict = {}
        html_attribute = type_config["html_field"]
        for item in course_content:
            html_string = getattr(item, html_attribute)
            content_name = getattr(
                course_content,
                type_config["title_field"],
                "Untitled",
            )
            new_key = content_name
            suffix = 0
            while new_key in html_data_dict:
                new_key = f"{content_name}_suffix_{suffix}"
                suffix += 1
            html_data_dict[new_key] = html_string
        return html_data_dict

    """,
    column=2, disabled=False, hide_code=False, name="_"
)


@app.function
def fetch_urls(course_content, content_type, config_dict):
    logger.info(f"Fetching urls for {content_type}")
    type_config = config_dict["content_types"].get(content_type)
    if not type_config:
        error_message = f"Unknown content type: {content_type}"
        raise ValueError(error_message)
    url_attribute = type_config["url_field"]
    return getattr(course_content,url_attribute,"URL not found")


@app.cell
def _():
    return


@app.cell
def _(course_obj):
    course_obj.__dict__
    return


app._unparsable_cell(
    r"""
    potential_a11y_issues = []
    for content_type, course_content_list in course_content_dict.items():
        if content_type == "Pages":
            print(content_type)
            base_url = f"https://boisestatecanvas.instructure.com/courses/{course_id}/pages/"
            for item in course_content_list:
                url = base_url + fetch_urls(item, content_type, config)
                print(f"URL: {url}")
        elif
    """,
    name="_"
)


@app.cell
def _(course_files, course_id):
    course_file_data = [
        {
            "course_id": course_id,
            "audit_status": "Not Yet Started",  # Change this to align with the existing spreadsheets.
            "content-type": file.__dict__.get(
                "content-type"
            ),  # See how this feels
            "display_name": file.__dict__.get("display_name"),
            "url": file.__dict__.get("url"),
            "reason_extracted": "All files should be checked.",  # This could be a mapping based on content-type
            "canvas_flags": "See Ally",  # This is useless. Consider renaming column.
            "canvas_details": "See Ally",  # This is useless. Consider renaming column.
            "alt_text": "N/A for raw files.",
            "run_id": RUN_ID,
            "hidden?": file.__dict__.get("hidden"),
            "storage_location":file.__dict__.get("folder_id"),
        }
        for file in course_files
    ]
    return


@app.cell
def _(
    content_name,
    content_type,
    content_url,
    course_id,
    file,
    potential_a11y_issues,
    soup,
):
    for a_tag in soup.find_all("a", href=True):
            link_url = a_tag["href"]
            link_text = a_tag.get_text(strip=True)
            potential_a11y_issues.append(
                {
                    "course_id": course_id,
                    "audit_status": "Not Yet Started",
                    "content-type": f"Link found in {content_type}",
                    "display_name": content_name, # Make sure this is the same as will show up in ally data
                    "url": content_url,
                    "reason_extracted": "Link", # This doesn't give more useful information
                    "canvas_flags": "Check link for descriptive text and functional link. Note: may link to an inaccessible resource. Encourage faculty to check.",
                    "canvas_details": f"Link to: {link_url} (Link text: '{link_text}')",
                    "alt_text": "N/A",
                    "run_id": RUN_ID,
                    "hidden?": file.__dict__.get("hidden"),
                    "published?": file.__dict__.get("published"),
                },
            )
    return


app._unparsable_cell(
    r"""
    for img_tag in soup.find_all("img", src=True):
            img_src = img_tag.get("src", "\")
            alt_text = img_tag.get("alt", "\")
            if not alt_text:
                issue = "Image Missing Alt Text"
            elif not alt_text.strip():
                issue = "Image With Empty Alt Text"
            else:
                issue = "Check Quality of Alt Text"
                logger.info(
                    f"Alt text found for {content_name}. Check quality of alt text.",
                )
            potential_a11y_issues.append(
                {
                    "course_id": course_id,
                    "audit_status": "Not Yet Started",
                    "content-type": f"Link found in {content_type}",
                    "display_name": content_name, # Change
                    "url": content_url,
                    "reason_extracted": "Image", # Change
                    "canvas_flags": issue,
                    "canvas_details": f"Image source: {img_src}",
                    "alt_text": alt_text,
                    "run_id": RUN_ID,
                    "hidden?": file.__dict__.get("hidden"),
                    "published?": file.__dict__.get("published"),
                },
            )

    """,
    name="_"
)


@app.cell
def _(
    content_name,
    content_type,
    content_url,
    course_id,
    file,
    potential_a11y_issues,
    soup,
):
    for iframe in soup.find_all("iframe"):
            potential_a11y_issues.append(
                {
                    "course_id": course_id,
                    "audit_status": "Not Yet Started", # Change
                    "content-type": content_type,
                    "display_name": content_name, # Change
                    "url": content_url,
                    "reason_extracted": "Embedded Media (Video)",
                    "canvas_flags": "May require manual caption check",
                    "canvas_details": f"Video source: {iframe['src']}",
                    "alt_text": "N/A",
                    "run_id": RUN_ID,
                    "hidden?": file.__dict__.get("hidden"),
                    "published?": file.__dict__.get("published"),
                },
            )
    return


app._unparsable_cell(
    r"""


        for video_tag in soup.find_all("video", src=True):
            potential_a11y_issues.append(
                {
                    "course_id": course_id,
                    "audit_status": "Not Yet Started", # Change
                    "content-type": content_type,
                    "display_name": content_name, # Change
                    "url": content_url,
                    "reason_extracted": "Embedded Media (Video)",
                    "canvas_flags": "May require manual caption check",
                    "canvas_details": f"Video source: {video_tag['src']}",
                    "alt_text": "N/A",
                    "run_id": RUN_ID,
                    "hidden?": file.__dict__.get("hidden"),
                    "published?": file.__dict__.get("published"),
                },
            )

        for audio_tag in soup.find_all("audio", src=True):
            potential_a11y_issues.append(
                {
                    "course_id": course_id,
                    "audit_status": "Not Yet Started", # Change
                    "content-type": content_type,
                    "display_name": content_name, # Change
                    "url": content_url,
                    "reason_extracted": "Embedded Media (Audio)",
                    "canvas_flags": "May require manual transcript check",
                    "canvas_details": f"Audio source: {audio_tag['src']}",
                    "alt_text": "N/A",
                    "run_id": RUN_ID,
                    "hidden?": file.__dict__.get("hidden"),
                    "published?": file.__dict__.get("published"),
                },
            )
    """,
    name="_"
)


@app.cell
def _(Assignments, Discussions, Pages):
    Pages
    ('method', 'get_pages')
    ('html_field', 'body')
    ('title_field', 'title')
    ('url_field', 'url')
    ('id_field', 'page_id')
    ('published_field', 'published')
    ('hidden_field', 'hide_from_students')
    ('keyword_params', {'include': ['body']})

    Assignments
    ('method', 'get_assignments')
    ('html_field', 'description')
    ('title_field', 'name')
    ('url_field', 'html_url')
    ('id_field', 'id')
    ('published_field', 'published')
    ('visbility_field', 'assignment_visibility')
    ('discussion_topic_field', 'discussion_topic')
    ('uses_rubric_field', 'use_rubric_for_grading')
    ('rubric_field', 'rubric')
    ('keyword_params', {'include': ['assignment_visibility']})
    Discussions
    ('method', 'get_discussion_topics')
    ('html_field', 'message')
    ('title_field', 'title')
    ('url_field', 'html_url')
    ('id_field', 'id')
    ('published_field', 'published')
    ('attachments_field', 'attachments')
    ('keyword_params', {})
    return


@app.cell(column=3)
def _():
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # Main Function Flow

    ```
    def main(
        config_path: Path = CONFIG_FILE,
        storage_file_path: str | Path = f"accessibility_review_{RUN_ID}.csv",
    ) -> str:
        "\"\"
        Orchestrates the process of extracting, cleaning, and saving course data.

        Accessibility data is pulled from Ally course report via the reports
        dashboard, and requires Institutional ID, Course ID, plus Ally Key & Secret.
        Canvas data is pulled from the Canvas API, and requires Institutional ID,
        Course ID, Canvas API Access Token, plus course design privileges for course.

        Parameters
        ----------
        config_path : Path, optional
            Path object pointing to the config file.
            By default, CONFIG_FILE
        storage_file_path : str | Path, optional
            String or Path object pointing to the config file.
            By default, f"accessibility_review_{RUN_ID}.csv"

        Returns
        -------
        str
            Message telling you whether or not the process succeeded.
        "\"\"
        logger.info("Starting program...")

        config = load_config(config_path)
        course_id = config.get("course_id")

        course_obj = initialize_canvas_course(config, course_id)
        logger.info(course_obj)
        course_content_dict = fetch_course_content(course_obj, config)
        logger.info(course_content_dict)
        course_files = course_content_dict.get("Files")
        course_file_data = parse_course_file_data(course_files, course_id)

        potential_a11y_issues = []
        for content_type, course_content in course_content_dict.items():
            if content_type != "Files":
                html_data_dict = extract_html(
                    course_content,
                    content_type,
                    config,
                )
                for name, html_string in html_data_dict.items():
                    cleaned_name = re.sub(r'_suffix_.*', '', name)
                    content_type_a11y_issues = parse_html_content(
                        html_string,
                        course_id,
                        content_type,
                        cleaned_name,
                        "URL_PLACEHOLDER",
                    )
                    potential_a11y_issues.extend(content_type_a11y_issues)
        canvas_df = create_canvas_data_df(
            course_file_data,
            potential_a11y_issues,
        )

        try:
            ally_csv_path = get_ally_report(course_id, config)
        except Exception as e:
            error_message = "Process failed during Ally report download."
            logger.error(f"{error_message}: {e}")
            return error_message
        ally_df = create_ally_df(
            ally_csv_path,
        ).pipe(clean_ally_df, config_dict=config)

        joint_df = join_data_sources(canvas_df, ally_df)
        save_as_csv(joint_df, storage_file_path)
        success_message = f"Congratulations! Accessibility Review File created: {storage_file_path}!"
        logger.success(success_message)
        return success_message
    ```
    """)
    return


@app.cell
def _():
    return


@app.cell(column=4)
def _():
    return


if __name__ == "__main__":
    app.run()
