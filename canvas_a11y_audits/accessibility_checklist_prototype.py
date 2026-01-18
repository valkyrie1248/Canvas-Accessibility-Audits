"""Retrieve and combine data for a single course from both ally and canvas."""

import sys
import tomllib
import datetime
import time
import requests
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd
from canvasapi import Canvas
from bs4 import BeautifulSoup
from loguru import logger
from pandas.core.frame import DataFrame
from playwright.sync_api import sync_playwright

if TYPE_CHECKING:
    from canvasapi.course import Course
    from canvasapi.file import File
    from canvasapi.paginated_list import PaginatedList
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
# SET-UP
# =============================================================================
PROJECT_ROOT = Path(__file__).parent.parent
config_file = PROJECT_ROOT / "config.toml"


# =============================================================================
# FUNCTIONS - Initialize Configurations
# =============================================================================
@logger.catch()
def load_config(config_path: Path=config_file) -> dict:
    f"""Parse toml file containing configuration and data schema rules.

    Parameters
    ----------
    config_path: Path
        Path object pointing to the config file. ({config_file}, by default)

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
        return {}

# =============================================================================
# FUNCTIONS - Download and Clean Course Data from Canvas
# =============================================================================
@logger.catch(
    message="Failed to initialize Canvas Course object. See traceback for details.",
)
def initialize_canvas_course(config_dict: dict, course_id: str) -> DataFrame:
    """Initialize Canvas Course object.

    Note: This does not download the full Canvas Course. Rather, it defers
    fetching until the later .get method is applied to it.

    Parameters
    ----------
    config_dict: dict
        Dictionary containing all rules, equivalencies, and settings

    course_id: str
        Unique Canvas identifier for the course you are reviewing.

    Returns
    -------
    course_obj: Course
        Course object (from CanvasAPI) for the chosen course
    """
    canvas_url = config_dict.get("canvas").get("url")
    canvas_token = config_dict.get("canvas").get("token")

    logger.info("Initializing Canvas object...")
    canvas: Canvas = Canvas(canvas_url, canvas_token)

    logger.info("Initializing Canvas Course object...")
    course_obj: Course = canvas.get_course(course_id)

    return course_obj


@logger.catch()
def fetch_course_content(course:Course, config_dict: dict) -> dict:
    '''Retrieve Canvas course data from Canvas API for all types in config.

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
    '''
    # TODO Add try/except loop.
    course_content_dict = {}
    config_content_types = config_dict.get("content_types")
    for content_type, params in config_content_types.items():
        logger.info(f"Fetching Canvas {content_type}")
        fetch_method = getattr(course, params["method"])
        course_content_dict.update(
            {content_type : fetch_method(**params["keyword_params"])}
            )
    return course_content_dict


@logger.catch()
def parse_course_file_data(course_files, course_id: str, run_id: int ) -> list[dict[str,str]]:
    '''Extract and organize important data about course files.

    Parameters
    ----------
    course_files: PaginatedList[File]
        PaginatedList of File objects assembled by CanvasAPI library.
    course_id: str
        Unique Canvas identifier for the course you are reviewing.
    run_id: int
        Unique program identifier for this run of the program.

    Returns
    -------
    course_file_data: list[dict[str,str|int]]
        List of dictionaries, with each dictionary corresponding to a single
        file in the course site that may have accessibility issues.
    '''
    logger.info("Fetching course files data...")

    course_file_data = []
    for file in course_files:
        course_file_data.append({
            "course_id": course_id,
            "audit_status": "New Content",
            "content-type": "File",
            "display_name": file.__dict__.get("display_name"),
            "url": file.__dict__.get("url"),
            "reason_extracted": file.__dict__.get("content-type"),
            "canvas_flags": "See Ally",
            "canvas_details": "See Ally",
            "alt_text": "N/A",
            "run_id": run_id,
        })
    logger.debug(len(course_file_data))
    return course_file_data


@logger.catch()
def extract_html(course_content, content_type, config) -> str:
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
        - 'quizzes'
        - 'discussions'
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
    """
    logger.info(f"Extracting html_data for {content_type}")
    type_config = config["content_types"].get(content_type)
    if not type_config:
        raise ValueError(f"Unknown content type: {content_type}")

    html_string = getattr(course_content, type_config["html_field"], "")
    content_name = getattr(course_content, type_config["title_field"], "Untitled")
    return html_string,content_name


# TODO Learn about iframe in Canvas and decide whether to include it.
@logger.catch()
def parse_html_content(
    html_string: str,
    course_id: str,
    run_id: int,
    content_type: str,
    content_name: str,
    content_url: str
    ) -> list[dict[str,str]]:
    '''
    Search html_content for items that need to be checked for accessibility.
    Returns a list of dictionaries, with each dictionary corresponding to a
    single item in the course site that may have accessibility issues.

    Parameters
    ----------
    html_string: _type_
        Raw HTML extracted from a course content object.
    course_id: str
        Unique Canvas identifier for the course you are reviewing.
    run_id: int
        Unique program identifier for this run of the program.
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
    '''

    potential_a11y_issues = []

    if not html_string:
        logger.debug("html_string is empty. Returning empty list...")
        return potential_a11y_issues
    else:
        logger.debug("html string not empty. Extracting now...")
    soup = BeautifulSoup(html_string, 'html.parser')

    for a_tag in soup.find_all('a', href=True):
        link_url = a_tag['href']
        link_text = a_tag.get_text(strip=True)
        potential_a11y_issues.append({
            "course_id": course_id,
            "audit_status": "New Content",
            "content-type": content_type,
            "display_name": content_name,
            "url": link_url,
            "reason_extracted": "Link",
            "canvas_flags": None,
            "canvas_details": f"Link to: {href} (Link text: '{text}')",
            "alt_text": "N/A",
            "run_id": run_id,
        })

    for img_tag in soup.find_all('img', src=True):
        img_src = img_tag.get('src','')
        alt_text = img_tag.get('alt', '')
        if not alt_text:
            issue = "Image Missing Alt Text"
        elif alt_text.strip() == "":
            issue = "Image With Empty Alt Text"
        else:
            issue = "Check Quality of Alt Text"
            logger.info(f"Alt text found for {content_name}. May want to check quality of alt text.")
        potential_a11y_issues.append({
            "course_id": course_id,
            "audit_status": "New Content",
            "content-type": content_type,
            "display_name": content_name,
            "url": content_url,
            "reason_extracted": "Image",
            "canvas_flags": issue,
            "canvas_details": f"Image source: {img_src}",
            "alt_text": alt_text,
            "run_id": run_id,
        })

    for iframe in soup.find_all('iframe'):
        potential_a11y_issues.append({
            "course_id": course_id,
            "audit_status": "New Content",
            "content-type": content_type,
            "display_name": content_name,
            "url": content_url,
            "reason_extracted": "Embedded Media (Video)",
            "canvas_flags": "May require manual caption check",
            "canvas_details": f"May require manual caption check {iframe['src']}",
            "alt_text": "N/A",
            "run_id": run_id,
        })

    for video_tag in soup.find_all('video', src=True):
        potential_a11y_issues.append({
            "course_id": course_id,
            "audit_status": "New Content",
            "content-type": content_type,
            "display_name": content_name,
            "url": content_url,
            "reason_extracted": "Embedded Media (Video)",
            "canvas_flags": "May require manual caption check",
            "canvas_details": f"May require manual caption check {video_tag['src']}",
            "alt_text": "N/A",
            "run_id": run_id,
        })

    for audio_tag in soup.find_all('audio', src=True):
        potential_a11y_issues.append({
            "course_id": course_id,
            "audit_status": "New Content",
            "content-type": content_type,
            "display_name": content_name,
            "url": content_url,
            "reason_extracted": "Embedded Media (Audio)",
            "canvas_flags": "May require manual caption check",
            "canvas_details": f"May require manual transcript check {audio_tag['src']}",
            "alt_text": "N/A",
            "run_id": run_id,
        })
    logger.debug(len(potential_a11y_issues))
    return potential_a11y_issues

# =============================================================================
# FUNCTIONS - Download Course Report from Ally
# =============================================================================
logger.catch("There was an error getting the session cookie: ")
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
    ally_config = config_dict.get("ally", {})
    key = ally_config.get("key")
    secret = ally_config.get("secret")

    if not key or not secret:
        raise ValueError("Missing 'key' or 'secret' in the [ally] section of your config.")

    target_url = "https://prod.ally.ac/report/11637"

    with sync_playwright() as p:
        # Launch browser (set to False to debug visually)
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        # 2. Navigate and Login
        page.goto(target_url)

        # Selectors based on your provided HTML
        page.fill('#key', key)
        page.fill('#secret', secret)
        page.click('button[type="submit"]')

        # 3. Wait/Poll for Cookie
        # Loop for up to 10 seconds checking for the cookie
        ally_cookie = None
        logger.info("Waiting for session cookie...")

        for _ in range(10):
            cookies = context.cookies()
            ally_cookie = next(
                (c for c in cookies if "ally.ac" in c["domain"] and "session" in c["name"]),
                None
            )
            if ally_cookie:
                break
            time.sleep(1)

        browser.close()

        if ally_cookie:
            success_message = f"{ally_cookie['name']}={ally_cookie['value']}"
            logger.success(success_message)
            return success_message
        else:
            raise ValueError("Failed to retrieve Ally session cookie. Check Key/Secret credentials.")


@logger.catch()
def trigger_ally_export(course_id: str, config_dict: dict, cookie_string:str) -> str:
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
    requests.HTTPError
        If the API call fails or times out after retries.
    KeyError
        If the response does not contain the expected 'url'.
    """
    ally_config = config_dict.get("ally", {})
    region = ally_config.get("region")
    client_id = ally_config.get("client_id")
    token = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M")

    if not all([client_id, token, cookie_string]):
        raise ValueError("Missing Ally configuration parameters (client_id or cookie_string).")

    base_url = f"https://{region}/api/v1/{client_id}/reports/courses/{course_id}/csv"
    params = {"token": token}
    headers = {"Cookie": cookie_string}

    logger.info(f"Triggering Ally report export for course {course_id} (Token: {token})...")

    retries = 3
    for attempt in range(retries):
        response = requests.get(base_url, params=params, headers=headers)

        if response.status_code == 200:
            logger.success("Ally report is ready.")
            data = response.json()
            return data["url"]

        elif response.status_code == 202:
            logger.info(f"Report processing (202). Retrying in 5 seconds... ({attempt + 1}/{retries})")
            time.sleep(5)
            continue

        else:
            response.raise_for_status()

    raise requests.exceptions.Timeout(f"Ally report processing timed out after {retries} retries.")


@logger.catch()
def download_s3_file(s3_url: str, course_id: str, download_dir: str) -> Path:
    """Download course report from S3 URL and save to the specified directory.

    Parameters
    ----------
    s3_url : str
        The signed S3 URL created by trigger_ally_export.
    course_id: str
        Unique Canvas identifier for the course. (Used for naming file.)
    download_dir : str
        Directory to save the report.

    Returns
    -------
    Path
        Path object pointing to the downloaded CSV file

    Raises
    ------
    ValueError
        If the downloaded file is suspiciously small, suggesting an error.
    """
    date_time = datetime.datetime.now().strftime("%Y%m%d%H%M")
    save_path = Path(download_dir)
    save_path.mkdir(parents=True, exist_ok=True)
    file_path = save_path / f"ally_{course_id}_{date_time}.csv"

    logger.info(f"Downloading S3 file to {file_path}...")

    # S3 signed URLs generally shouldn't have extra headers like cookies
    headers = {"User-Agent": "Mozilla/5.0"}

    with requests.get(s3_url, headers=headers, stream=True) as r:
        r.raise_for_status()
        with open(file_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    # Data Integrity Check
    file_size = file_path.stat().st_size
    if file_size <= 1000:
        logger.error(f"Downloaded file is suspiciously small ({file_size} bytes). Likely an S3 XML error.")
        raise ValueError("Downloaded file integrity check failed (size < 1000 bytes).")

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
    download_dir = config_dict.get("ally", {}).get("download_dir", "./downloads/ally_reports")
    session_cookie = get_ally_session_cookie(config_dict)

    s3_url = trigger_ally_export(course_id, config_dict, session_cookie)
    csv_path = download_s3_file(s3_url, course_id, download_dir)

    return csv_path

# =============================================================================
# FUNCTIONS - DataFrame Creation
# =============================================================================
@logger.catch("Failed to craete Canvas api DataFrame")
def create_canvas_data_df(
    config_dict: dict,
    course_file_data: list[dict[str,str]],
    potential_a11y_issues: list[dict[str,str]],
    dtypebackend = "pyarrow"
    ) -> DataFrame:
    """Create a DataFrame of canvas items with potential accessibility issues.

    Parameters
    ----------
    config_dict: dict
        Dictionary containing all rules, equivalencies, and settings
    course_file_data: list[dict[str,str|int]]
        List of dictionaries, with each dictionary corresponding to a single
        file in the course site that may have accessibility issues.
    potential_a11y_issues: list[dict[str,str]]
        List of dictionaries, with each dictionary corresponding to a single
        item in the course site that may have accessibility issues.
    dtypebackend: str, optional
        The dtype backend you want for your dataframe ("pyarrow", by default)

    Returns
    -------
    canvas_df: DataFrame
        A DataFrame where each row is a single canvas item that may have an
        accessibility issue.
    """
    potential_a11y_issues.extend(course_file_data)
    logger.debug(len(potential_a11y_issues))

    # drop_cols = config_dict.get("canvas").get("drop_columns")

    logger.info("Initializing DataFrame")
    canvas_df: DataFrame = (
        pd.DataFrame(potential_a11y_issues)
        .convert_dtypes(dtype_backend=dtypebackend)
        )
    # canvas_df = canvas_df.drop(drop_cols,axis=columns) I think this is now not needed.
    logger.debug(canvas_df.shape)
    return canvas_df


@logger.catch(
    message="Failed to load ally DataFrame. See traceback for details.",
)
def create_ally_df(
    file_path: str,
    dtypebackend: str = "pyarrow",
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
    if show_deleted == True:
        logger.debug(ally_df.shape)
        return ally_df
    logger.debug(f"shape of ally_df = {ally_df.shape}")
    return ally_df.loc[ally_df["Deleted at"].isna()]


@logger.catch()
def clean_ally_df(ally_df:DataFrame, config_dict:dict) -> DataFrame:
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
        raise ValueError("No flag columns found in the configuration.")

    flagged_df = ally_df[flags]
    flagged_df = flagged_df[flagged_df == 1]
    flag_list = (
    flagged_df
    .reset_index()
    .melt(id_vars='index', value_vars=flags)
    .dropna()
    .groupby("index")["variable"]
    .agg(", ".join)
    )
    ally_df['Flags'] = ally_df.index.map(flag_list)
    clean_ally_df = ally_df.drop(columns=flags)
    return clean_ally_df


@logger.catch(message="Failed to join DataFrames. See traceback for details.")
def join_data_sources(
    canvas_df: DataFrame,
    ally_df: DataFrame,
    drop_cols: list[str] | None = None,
    ) -> DataFrame:
    """Join Ally and Canvas DataFrames on their corresponding file name columns.

    Parameters
    ----------
    canvas_df: DataFrame
        DataFrame of all files from Canvas course site. (Could also be all content from canvas if you create it right.)
    ally_df: DataFrame
        DataFrame of all content in Canvas course site that Ally has checked for accessibility.
    drop_cols: list[str], optional
        List of columns you want to drop from the final DataFrame.
        By default, None.

    Returns
    -------
    joint_df: DataFrame
        A pandas DataFrame that combines the data from Canvas and Ally.

    """
    joint_df = pd.merge(
        canvas_df.copy(),
        ally_df.copy(),
        on="display_name",
        how="outer",
    )
    logger.debug(f"shape of joint_df = {joint_df.shape}")
    logger.debug(f"info for joint_df: {joint_df.info()}")
    return (joint_df
            # .dropna(subset="Score") I think this might lose some important things now...
            # .drop(drop_cols,axis="columns",)
            )


@logger.catch(
    message="Failed to write {df} to CSV. See trackeback for details.",
)
def save_as_csv(df: DataFrame,file_path: Path | str) -> None:
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
    date_time = datetime.datetime.now().strftime("%Y%m%d%H%M")
    df.loc[df["Score"].notna(),"Score"] = df.loc[df["Score"].notna(),"Score"] * 100
    return df.to_csv(
        file_path,
        na_rep="",
        mode="x",
        float_format="%.2f",
    )

# TODO Use conditional to skip parse-extract func w/files.
# TODO Move some of this doc string to the README?
@logger.catch()
def main(
    config_path: Path = config_file,
    run_id: int = int(datetime.datetime.now().timestamp()),
    storage_file_path: str | Path = f"accessibility_review_{date_time}.csv"
    ) -> str:
    """
    Orchestrates the process of extracting, cleaning, and saving course data.

    Accessibility data is pulled from Ally course report via the reports
    dashboard, and requires Institutional ID, Course ID, plus Ally Key & Secret.
    Canvas data is pulled from the Canvas API, and requires Institutional ID,
    Course ID, Canvas Bearer Token, plus course design privileges for course.

    Parameters
    ----------
    config_path : Path, optional
        Path object pointing to the config file.
        By default, config_file
    run_id : int, optional
        Unique program identifier for this run of the program.
        By default, int(datetime.datetime.now().timestamp())
    storage_file_path : str | Path, optional
        String or Path object pointing to the config file.
        By default, f"accessibility_review_{date_time}.csv"

    Returns
    -------
    str
        Message telling you whether or not the process succeeded.
    """
    logger.info("Starting program...")

    config = load_config(config_path)
    course_id = config.get("course_id")

    course_obj = initialize_canvas_course(config, course_id)

    course_content_dict = fetch_course_content(course_obj, config)

    course_files = course_content_dict.get("Files")
    course_file_data = parse_course_file_data(course_files,course_id, run_id)

    potential_a11y_issues = []
    for content_type,course_content in course_content_dict.items():
        if content_type != "Files":
            html_string,content_name = extract_html(course_content,content_type,config)
            content_type_a11y_issues = parse_html_content(html_string,course_id,content_type,content_name,"URL_PLACEHOLDER")
            potential_a11y_issues.extend(content_type_a11y_issues)
    canvas_df = create_canvas_data_df(config,course_file_data,potential_a11y_issues)

    try:
        ally_csv_path = get_ally_report(course_id, config)
    except Exception as e:
        logger.error(f"Failed to download Ally report: {e}")
        return "Process failed during Ally report download."
    ally_df = create_ally_df(
        ally_csv_path,
    ).pipe(clean_ally_df, config_dict=config)

    drop_joint_columns = config.get("joint").get("drop_columns")
    joint_df = join_data_sources(canvas_df, ally_df, drop_joint_columns)
    save_as_csv(joint_df,storage_file_path)
    success_message = f"Congratulations! Accessibility Review File created: {storage_file_path}!"
    logger.success(success_message)
    return success_message


# =============================================================================
# EXECUTION - Orchestrator
# =============================================================================
if __name__ == "__main__":
    success_message = main()
    print(success_message)