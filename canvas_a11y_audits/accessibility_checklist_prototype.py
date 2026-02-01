"""Compiles a spreadsheet for course accessibility audits from both Canvas and Ally API data.

Copyright (C) 2026  Jeremy Harper (valkyrie1248@protonmail.com).

This program is free software: you can redistribute it and/or modify it under
the terms of the GNU Affero General Public License as published by the Free
Software Foundation, either version 3 of the License, or any later version.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE.  See the GNU Affero General Public License for
more details.

You should have received a copy of the GNU Affero General Public License along
with this program.  If not, see <https://www.gnu.org/licenses/>.

If your software can interact with users remotely through a computer network,
you should also make sure that it provides a way for users to get its source.
For example, if your program is a web application, its interface could display
a "Source" link that leads users to an archive of the code.  There are many
ways you could offer source, and different solutions will be better for
different programs; see section 13 for the specific requirements.
"""

import os
import re
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
    level="INFO",
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


# =============================================================================
# FUNCTIONS - Initialize Configuration
# =============================================================================
@logger.catch()
def load_config(config_path: Path) -> dict[str, str | dict[str, str | list[str | dict[str, str]]]]:
    """Parse toml file containing configuration and data schema rules.

    Parameters
    ----------
    config_path: Path
        Path object pointing to the config file.

    Returns
    -------
    dict
        Dictionary containing all rules, equivalencies, and configuration settings

    Raises
    ------
    FileNotFoundError
        If config file is not found at config_path
    """
    try:
        with Path(config_path).open("rb") as f:
            return tomllib.load(f)
    except FileNotFoundError:
        logger.exception(f"Config file {config_path} not found.")
        raise


# =============================================================================
# FUNCTIONS - Download and Clean Course Data from Canvas
# =============================================================================
@logger.catch(
    message="Failed to initialize Canvas Course object. See traceback for details.",
)
def initialize_canvas_course(
    config_dict: dict[str, str | dict[str, str | list[str | dict[str, str]]]],
    course_id: str,
    canvas_token: str | None = CANVAS_TOKEN,
) -> DataFrame:
    """Initialize Canvas Course object.

    Note: This does not download the full Canvas Course. Rather, it defers
    fetching until the later .get method is applied to it.

    Parameters
    ----------
    config_dict: dict
        Dictionary containing all rules, equivalencies, and settings

    course_id: str
        Unique Canvas identifier for the course you are reviewing.

    canvas_token: str, optional
        Canvas API access token. Must be generated for individual user in Canvas
        user settings. (See https://community.instructure.com/en/kb/articles/662901-how-do-i-manage-api-access-tokens-in-my-user-account)
        By default, CANVAS_TOKEN.

    Returns
    -------
    course_obj: Course
        Course object (from CanvasAPI) for the chosen course
    """
    canvas_url = config_dict.get("canvas").get("url")

    logger.info("Initializing Canvas object...")
    canvas: Canvas = Canvas(canvas_url, canvas_token)

    logger.info("Initializing Canvas Course object...")
    course_obj: Course = canvas.get_course(course_id)

    return course_obj


@logger.catch()
def fetch_course_content(course: Course, config_dict: dict) -> dict:
    """Retrieve Canvas course data from Canvas API for all types in config.

    Parameters
    ----------
    course: Course
        Canvas Course object created by CanvasAPI library.

    config_dict: dict
        Dictionary containing all rules, equivalencies, and settings

    Returns
    -------
    course_content_dict
        Dictionary where keys are content types and values are results of
        the corresponding CanvasAPI course.get_{content_type} calls.

    Raises
    ------
    ValueError
        If config_dict does not contain content types dictionary.
    """
    course_content_dict = {}
    config_content_types: dict[str, dict] | None = config_dict.get("content_types")  # pyright: ignore[reportMissingTypeArgument]
    if not config_content_types:
        error_message = "Failed to load content types. Check config.toml."
        raise ValueError(error_message)
    for content_type, params in config_content_types.items():
        fetch_method = getattr(course, params["method"])
        logger.debug(fetch_method)
        course_content_dict.update(
            {content_type: fetch_method(**params["keyword_params"])},
        )
        logger.debug(f"course_content_dict now contains {course_content_dict}")
    return course_content_dict


@logger.catch()
def parse_course_file_data(
    course_files: PaginatedList[File],
    course_id: str,
) -> list[dict[str, str]]:
    """Extract and organize important data about course files.

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
    """
    logger.info("Fetching course files data...")

    course_file_data = [
        {
            "course_id": course_id,
            "audit_status": "Not Yet Started",
            "content_type": file.__dict__.get(
                "content-type",
            ),  # Could this go to Mime_type and have simplified type here?
            "display_name": file.__dict__.get("display_name"),
            "url": file.__dict__.get("url"),
            "reason_extracted": "File",  # This could be a mapping based on content_type
            "canvas_flags": "See Ally",  # This is useless. Consider renaming column.
            "canvas_details": f"Stored in folder: {file.__dict__.get('folder_id')}",  # This is useless. Consider renaming column.
            "alt_text": "N/A for raw files.",
            "run_id": RUN_ID,
            "hidden?": file.__dict__.get("hidden"),
            "published?": "??",  # Check this out
        }
        for file in course_files
    ]
    logger.debug(len(course_file_data))
    for i, item in enumerate(course_file_data):
        logger.debug(f"Index {i}: {item}")
    return course_file_data


@logger.catch()
def fetch_urls(course_content, content_type, config_dict):
    logger.info(f"Fetching urls for {content_type}")
    type_config = config_dict["content_types"].get(content_type)
    if not type_config:
        error_message = f"Unknown content type: {content_type}"
        raise ValueError(error_message)
    url_attribute = type_config["url_field"]
    return getattr(course_content, url_attribute, "URL not found")


@logger.catch()
def extract_html(course_content, content_type: str, config_dict: dict) -> str:
    """
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
    html_data_dict: dict[str,str]
        Dictionary containing title and raw html string.

    Raises
    ------
    ValueError
        If content_type is not one of the types outlined in config file.
    """
    logger.info(f"Extracting html_data for {content_type}")
    type_config = config_dict["content_types"].get(content_type)
    if not type_config:
        error_message = f"Unknown content type: {content_type}"
        raise ValueError(error_message)
    html_data_dict = {}
    html_attribute = type_config["html_field"]
    name_attribute = type_config["title_field"]
    for item in course_content:
        html_string = getattr(item, html_attribute)
        content_name = getattr(
            item,
            name_attribute,
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
    """
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
    """
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
                "audit_status": "Not Yet Started",
                "content_type": f"Link found in {content_type}",
                "display_name": content_name,  # Make sure this is the same as will show up in ally data
                "url": content_url,
                "reason_extracted": "Link",  # This doesn't give more useful information
                "canvas_flags": "Check link for descriptive text and functional link. Note: may link to an inaccessible resource. Encourage faculty to check.",
                "canvas_details": f"Link to: {link_url} (Link text: '{link_text}')",
                "alt_text": "N/A",
                "run_id": RUN_ID,
                # "hidden?": file.__dict__.get("hidden"),
                # "published?": file.__dict__.get("published"),
            },
        )

    for img_tag in soup.find_all("img", src=True):
        img_src = img_tag.get("src", "")
        alt_text = img_tag.get("alt", "")
        if not alt_text:
            issue = "Image Missing Alt Text"
        elif not alt_text.strip():
            issue = "Image With Empty Alt Text"
        else:
            issue = "Check Quality of Alt Text"
            logger.debug(
                f"Alt text found for {content_name}. Check quality of alt text.",
            )
        potential_a11y_issues.append(
            {
                "course_id": course_id,
                "audit_status": "Not Yet Started",
                "content_type": f"Link found in {content_type}",
                "display_name": content_name,  # Change
                "url": content_url,
                "reason_extracted": "Image",  # Change
                "canvas_flags": issue,
                "canvas_details": f"Image source: {img_src}",
                "alt_text": alt_text,
                "run_id": RUN_ID,
                # "hidden?": file.__dict__.get("hidden"),
                # "published?": file.__dict__.get("published"),
            },
        )

    for iframe in soup.find_all("iframe"):
        potential_a11y_issues.append(
            {
                "course_id": course_id,
                "audit_status": "Not Yet Started",  # Change
                "content_type": content_type,
                "display_name": content_name,  # Change
                "url": content_url,
                "reason_extracted": "Embedded Media (Video)",
                "canvas_flags": "May require manual caption check",
                "canvas_details": f"Video source: {iframe['src']}",
                "alt_text": "N/A",
                "run_id": RUN_ID,
                # "hidden?": file.__dict__.get("hidden"),
                # "published?": file.__dict__.get("published"),
            },
        )

    for video_tag in soup.find_all("video", src=True):
        potential_a11y_issues.append(
            {
                "course_id": course_id,
                "audit_status": "Not Yet Started",  # Change
                "content_type": content_type,
                "display_name": content_name,  # Change
                "url": content_url,
                "reason_extracted": "Embedded Media (Video)",
                "canvas_flags": "May require manual caption check",
                "canvas_details": f"Video source: {video_tag['src']}",
                "alt_text": "N/A",
                "run_id": RUN_ID,
                # "hidden?": file.__dict__.get("hidden"),
                # "published?": file.__dict__.get("published"),
            },
        )

    for audio_tag in soup.find_all("audio", src=True):
        potential_a11y_issues.append(
            {
                "course_id": course_id,
                "audit_status": "Not Yet Started",  # Change
                "content_type": content_type,
                "display_name": content_name,  # Change
                "url": content_url,
                "reason_extracted": "Embedded Media (Audio)",
                "canvas_flags": "May require manual transcript check",
                "canvas_details": f"Audio source: {audio_tag['src']}",
                "alt_text": "N/A",
                "run_id": RUN_ID,
                # "hidden?": file.__dict__.get("hidden"),
                # "published?": file.__dict__.get("published"),
            },
        )
    logger.debug(len(potential_a11y_issues))
    return potential_a11y_issues


# =============================================================================
# FUNCTIONS - Download Course Report from Ally
# =============================================================================
@logger.catch("There was an error getting the session cookie: ")
def get_ally_session_cookie(config_dict: dict) -> str:
    """
    Retrieve session cookie from Ally institutional reporting dashboard.

    This session cookie is used for authentication when accessing course report
    API endpoint. Cookie must be for an active session, so Playwright navigates
    to the site in headless browser mode (i.e., you won't see this happen).
    Will wait up to 10 seconds to find session cookie before raising error.

    Parameters
    ----------
    config_dict: dict
        Dictionary containing all rules, equivalencies, and settings

    Returns
    -------
    cookie_string : str
        The formatted session cookie string required for API requests. Begins
        with "session-{client_id}=" and is followed by a very long string.

    Raises
    ------
    ValueError
        If 'key' or 'secret' are missing from the configuration.
        If the session cookie is not found after the login attempt.
    """
    # 1. Retrieve credentials
    key = ALLY_KEY
    secret = ALLY_SECRET

    if not key or not secret:
        error_message = "Key or Secret not found. Check .env file."
        raise ValueError(error_message)

    target_url = "https://prod.ally.ac/report/11637"

    with sync_playwright() as p:
        # Launch browser (set to False to debug visually)
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        # 2. Navigate and Login
        page.goto(target_url)

        # Selectors based on your provided HTML
        page.fill("#key", key)
        page.fill("#secret", secret)
        page.click('button[type="submit"]')

        # 3. Wait/Poll for Cookie
        # Loop for up to 30 seconds checking for the cookie
        ally_cookie = None
        logger.info("Waiting for session cookie...")

        for _ in range(30):
            cookies = context.cookies()
            ally_cookie = next(
                (c for c in cookies if "ally.ac" in c["domain"] and "session" in c["name"]),
                None,
            )
            if ally_cookie:
                break
            time.sleep(1)

        browser.close()

        if ally_cookie:
            success_message = f"{ally_cookie['name']}={ally_cookie['value']}"
            logger.success(success_message)
            return success_message
        error_message = "Failed to retrieve Ally session cookie. Check Key/Secret credentials."
        raise ValueError(error_message)


@logger.catch()
def trigger_ally_export(
    course_id: str,
    config_dict: dict,
    cookie_string: str,
    retries: int = 10,
) -> str:
    """Trigger the Ally CSV export via API and retrieve the S3 download URL.

    Uses the exposed API endpoint normally activated by clicking the export
    button in course-level report section of institutional reports dashboard.

    Parameters
    ----------
    course_id: str
        Unique Canvas identifier for the course you are reviewing.
    config_dict: dict
        Dictionary containing all rules, equivalencies, and settings
    cookie_string: str
        Cookie string that needs to be sent in header for authentication.

    Returns
    -------
    str
        The signed S3 URL for the requested report.

    Raises
    ------
    ValueError
        If Ally configuration parameters are missing.
    requests.HTTPError
        If the API call fails or times out after retries.
    KeyError
        If the response does not contain the expected 'url'.
    requests.exceptions.Timeout
        If the response times out after {retries} retries.
    """
    ally_config = config_dict.get("ally", {})
    region = ally_config.get("region")
    client_id = ally_config.get("client_id")
    token = datetime.now(tz=tz.tzlocal()).strftime("%Y-%m-%d-%H-%M")

    if not all([client_id, token, cookie_string]):
        error_message = "Missing Ally configuration parameters (client_id or cookie_string)."
        raise ValueError(error_message)

    base_url = f"https://{region}/api/v1/{client_id}/reports/courses/{course_id}/csv"
    params = {"token": token}
    headers = {"Cookie": cookie_string}

    status_success = 200
    status_processing = 202

    logger.info(
        f"Triggering Ally report export for course {course_id} (Token: {token})...",
    )

    for attempt in range(retries):
        response = requests.get(base_url, params=params, headers=headers)

        if response.status_code == status_success:
            logger.success("Ally report is ready.")
            data = response.json()
            return data["url"]

        if response.status_code == status_processing:
            logger.info(
                f"Report processing (202). Retrying in 10 seconds... ({attempt + 1}/{retries})",
            )
            time.sleep(10)
            continue

        response.raise_for_status()
    error_message = f"Ally report processing timed out after {retries} retries."
    raise requests.exceptions.Timeout(error_message)


@logger.catch()
def download_s3_file(
    s3_url: str,
    course_id: str,
    download_dir: str,
    min_file_size: int = 1000,
) -> Path:
    """Download course report from S3 URL and save to the specified directory.

    Parameters
    ----------
    s3_url: str
        The signed S3 URL created by trigger_ally_export.
    course_id: str
        Unique Canvas identifier for the course. (Used for naming file.)
    download_dir: str
        Directory to save the report.
    min_file_size: str, optional
        Minimum file size to be presumed more than just an error message.

    Returns
    -------
    Path
        Path object pointing to the downloaded CSV file

    Raises
    ------
    ValueError
        If the downloaded file is suspiciously small, suggesting an error.
    """
    save_path = Path(download_dir)
    save_path.mkdir(parents=True, exist_ok=True)
    file_path = save_path / f"ally_{course_id}_{RUN_ID}.csv"

    logger.info(f"Downloading S3 file to {file_path}...")

    # S3 signed URLs generally shouldn't have extra headers like cookies
    headers = {"User-Agent": "Mozilla/5.0"}

    with requests.get(s3_url, headers=headers, stream=True) as r:
        r.raise_for_status()
        with Path(file_path).open("wb") as f:
            f.writelines(r.iter_content(chunk_size=8192))

    # Data Integrity Check
    file_size = file_path.stat().st_size
    if file_size <= min_file_size:
        logger.error(
            f"Downloaded file is suspiciously small ({file_size} bytes). Likely an S3 XML error.",
        )
        error_message = "Downloaded file integrity check failed (size < 1000 bytes)."
        raise ValueError(error_message)

    logger.success(f"Successfully downloaded Ally report: {file_path} ({file_size} bytes)")
    return file_path


@logger.catch()
def get_ally_report(course_id: str, config_dict: dict) -> Path:
    """Fetch course report from Ally institutional report.

    Orchestrator that fetches the cookie string for authentication, then ensures
    trigger_ally_export and download_s3_file are executed in quick succession.
    This is how we bypass the short 120s expiration of signed s3 url.

    Parameters
    ----------
    course_id: str
        Unique Canvas identifier for the course you are reviewing.
    config_dict: dict
        Dictionary containing all rules, equivalencies, and settings

    Returns
    -------
    Path
        The path to the valid downloaded CSV report.
    """
    logger.info(f"Starting Ally report retrieval for Course ID: {course_id}")

    # Get download directory from config or default
    download_dir = config_dict.get("ally", {}).get(
        "download_dir",
        "./downloads/ally_reports",
    )
    session_cookie = get_ally_session_cookie(config_dict)

    s3_url = trigger_ally_export(course_id, config_dict, session_cookie)
    return download_s3_file(s3_url, course_id, download_dir)


# =============================================================================
# FUNCTIONS - DataFrame Creation
# =============================================================================
@logger.catch("Failed to craete Canvas api DataFrame")
def create_canvas_data_df(
    course_file_data: list[dict[str, str]],
    potential_a11y_issues: list[dict[str, str]],
    dtypebackend="pyarrow",
) -> DataFrame:
    """Create a DataFrame of canvas items with potential accessibility issues.

    Parameters
    ----------
    course_file_data: list[dict[str,str|int]]
        List of dictionaries, with each dictionary corresponding to a single
        file in the course site that may have accessibility issues.
    potential_a11y_issues: list[dict[str,str]]
        List of dictionaries, with each dictionary corresponding to a single
        item in the course site that may have accessibility issues.
    dtypebackend: str, optional
        The dtype backend you want for your dataframe
        By defualt, "pyarrow".

    Returns
    -------
    canvas_df: DataFrame
        A DataFrame where each row is a single canvas item that may have an
        accessibility issue.
    """
    for i, item in enumerate(potential_a11y_issues):
        if not isinstance(item, dict):
            logger.debug(f"Index {i} in potential_ally_issues is not a dict: {item}")
    for i, item in enumerate(course_file_data):
        if not isinstance(item, dict):
            logger.debug(f"Index {i} in course_file_data is not a dict: {item}")
    logger.info("Initializing DataFrame")
    potential_a11y_issues.extend(course_file_data)
    log_message = f"{len(potential_a11y_issues)} potential accessibility issues"

    logger.info("Initializing DataFrame")
    canvas_df: DataFrame = pd.DataFrame(potential_a11y_issues).convert_dtypes(
        dtype_backend=dtypebackend,
    )
    logger.debug(canvas_df.shape)
    return canvas_df


@logger.catch(
    message="Failed to load ally DataFrame. See traceback for details.",
)
def create_ally_df(
    file_path: str,
    dtypebackend: str = "pyarrow",
    *,
    use_columns: list[str] | None = None,
    show_deleted: bool = False,
) -> DataFrame:
    """Create a Pandas DataFrame from a downloaded Ally course report.

    Parameters
    ----------
    file_path: Path | str
        The full or relative path of the downloaded course report.
    use_columns: list[str], optional
        Set of columns to include.
        By default includes all columns
    dtypebackend: str, optional
        The dtype backend you want for your dataframe.
        By default, pyarrow (for performance and memory optimization)
    show_deleted: bool, optional
        Switch to True to include content that has been deleted from course site.
        By default, False

    Returns
    -------
    ally_df: DataFrame
        A pandas DataFrame

    """
    try:
        ally_df = pd.read_csv(
            file_path,
            dtype_backend=dtypebackend,
            usecols=use_columns,
            engine="pyarrow",
        ).rename(columns={"Name": "display_name"})
    except Exception as e:
        logger.critical(f"Pandas load failed: {e}")
        raise
    # ally_df[["Item type","id"]]=ally_df["Id"].str.split(":",expand=True)
    # ally_df=ally_df.set_index("id")
    if show_deleted:
        logger.debug(ally_df.shape)
        return ally_df

    ally_df = ally_df.loc[ally_df["Deleted at"].isna()].drop(columns=["Deleted at", "Checked on"])
    logger.debug(f"shape of ally_df = {ally_df.shape}")
    return ally_df


@logger.catch()
def clean_ally_df(ally_df: DataFrame, config_dict: dict) -> DataFrame:
    """
    Make Ally DataFrame more useful for review and tracking of remediation.

    This condenses the 38 columns for possible accessibility flags from Ally
    into a single column that that lists all flags for that row (item).

    Parameters
    ----------
    ally_df: DataFrame
        DataFrame of all content in Canvas course site that Ally has checked for accessibility.
    config_dict: dict
        Dictionary containing all rules, equivalencies, and settings

    Returns
    -------
    clean_ally_df: DataFrame
        Tidier version of the Ally data.

    Raises
    ------
    ValueError
        If it can't find the flag_columns in the config_dict.
    """
    flags = config_dict.get("ally").get("flag_columns")
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
    return cleaned_ally_df


@logger.catch(message="Failed to join DataFrames. See traceback for details.")
def join_data_sources(
    canvas_df: DataFrame,
    ally_df: DataFrame,
) -> DataFrame:
    """Join Ally and Canvas DataFrames on their corresponding file name columns.

    Parameters
    ----------
    canvas_df: DataFrame
        DataFrame of all files from Canvas course site.
        (Could also be all content from canvas if you create it right.)
    ally_df: DataFrame
        DataFrame of all content in Canvas course site that Ally has checked for accessibility.

    Returns
    -------
    joint_df: DataFrame
        A pandas DataFrame that combines the data from Canvas and Ally.

    """
    joint_df = canvas_df.copy().merge(
        ally_df.copy(),
        on="display_name",
        how="outer",
    )
    logger.debug(f"shape of joint_df = {joint_df.shape}")
    logger.debug(f"info for joint_df: {joint_df.info()}")
    return joint_df


@logger.catch(
    message="Failed to write {df} to CSV. See trackeback for details.",
)
def save_as_csv(df: DataFrame, file_path: Path | str) -> None:
    """
    Save the joint DataFrame as a csv.

    Parameters
    ----------
    df : DataFrame
        _description_
    file_path : Path | str
        _description_

    Returns
    -------
    None
    """
    df.loc[df["Score"].notna(), "Score"] *= 100
    return df.to_csv(
        file_path,
        na_rep="",
        mode="x",
        float_format="%.2f",
    )


@logger.catch()
def main(
    config_path: Path = CONFIG_FILE,
    storage_file_path: str | Path = f"accessibility_review_{RUN_ID}.csv",
) -> str:
    """
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
    """
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
        url = "URL not found"
        if content_type == "Pages":
            base_url = f"https://boisestatecanvas.instructure.com/courses/{course_id}/pages/"
            for item in course_content:
                url = base_url + fetch_urls(item, content_type, config)
                logger.debug(f"URL: {url}")
        elif content_type in ["Assignments", "Discussions"]:
            for item in course_content:
                url = fetch_urls(item, content_type, config)
                logger.debug(f"URL: {url}")

        if content_type != "Files":
            html_data_dict = extract_html(
                course_content,
                content_type,
                config,
            )
            for name, html_string in html_data_dict.items():
                cleaned_name = re.sub(r"_suffix_.*", "", name)
                content_type_a11y_issues = parse_html_content(
                    html_string,
                    course_id,
                    content_type,
                    cleaned_name,
                    url,
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


# =============================================================================
# EXECUTION - Orchestrator
# =============================================================================
if __name__ == "__main__":
    success_message = main()
    print(success_message)
