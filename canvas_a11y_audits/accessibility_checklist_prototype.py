"""Retrieve and combine data for a single course from both ally and canvas."""

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
# FUNCTIONS
# =============================================================================
@logger.catch()
def load_config(config_path=config_file) -> dict:
    """Parse toml file containing configuration and data schema rules.

    Return:
    Dictionary containing all rules, equivalencies, and configuration settings.

    """
    try:
        with Path(config_path).open("rb") as f:
            return tomllib.load(f)
    except FileNotFoundError:
        logger.exception(f"Config file {config_path} not found.")
        return {}


@logger.catch(
    message="Failed to initialize Canvas Course object. See traceback for details.",
)
def initialize_canvas_course(config_dict) -> DataFrame:
    """Initialize Canvas Course object.

    Note: This does not download the full Canvas Course. Rather, it defers
    fetching until the later .get method is applied to it.

    Parameters
    ----------
    config_dict: dict
        Dictionary of configuration settings from config.toml.

    Returns
    -------
    course_obj: Course
        Course object (from CanvasAPI) for the chosen course.
    """
    canvas_url = config_dict.get("canvas").get("url")
    canvas_token = config_dict.get("canvas").get("token")

    logger.info("Initializing Canvas object...")
    canvas: Canvas = Canvas(canvas_url, canvas_token)

    logger.info("Initializing Canvas Course object...")
    course_id = config_dict.get("course_id")
    course_obj: Course = canvas.get_course(course_id)

    return course_obj


@logger.catch()
def fetch_course_content(course:Course, config: dict):
    '''
    Retrieve Canvas course data from Canvas API for all types listed in config.

    Parameters
    ----------
    course: Course
        Canvas Course object created by CanvasAPI library.
    content_type: str
        String alias for the content type you want to fetch. Acceptable inputs:
        - 'pages'
        - 'assignments'
        - 'quizzes'
        - 'discussions'
        - 'files'
    config: dict
        Dictionary of configuration settings from config.toml.

    Returns
    -------
    course_content_dict
        Dictionary where keys are content types and values are results of
        the corresponding CanvasAPI course.get_{content_type} calls.
    '''
    # TODO Add try/except loop, Finish docstring.
    course_content_dict = {}
    config_content_types = config.get("content_types")
    for content_type, params in config_content_types.items():
        logger.info(f"Fetching Canvas {content_type}")
        fetch_method = getattr(course, params["method"])
        course_content_dict.update(
            {content_type : fetch_method(**params["keyword_params"])}
            )
    return course_content_dict


@logger.catch()
def parse_course_file_data(course_files, course_id) -> list[dict[str,str]]:
    '''
    Pull out portions of data for course files that will end up in our DataFrame

    Parameters
    ----------
    course_files: PaginatedList[File]
        PaginatedList of File objects assembled by CanvasAPI library.
    course_id: str
        course_id

    Returns
    -------
    course_file_data: list[dict[str,str|int]]
        List of dictionaries, with each dictionary corresponding to a single
        file in the course site that may have accessibility issues.
    '''
    logger.info("Fetching course files data...")
    run_id = int(datetime.datetime.now().timestamp())

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
    Extract html data from course content object for scraping.
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
    config: dict
        Dictionary of configuration settings from config.toml

    Returns
    -------
    html_string: str
        Raw HTML data for the content

    Raises
    ------
    ValueError
        _description_
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
def parse_html_content(html_string, course_id, content_type, content_name, content_url) -> list[dict[str,str]]:
    '''
    Search html_content for items that need to be checked for accessibility.
    Returns a list of dictionaries, with each dictionary corresponding to a
    single item in the course site that may have accessibility issues.

    Parameters
    ----------
    html_string: _type_
        _description_
    course_id: _type_
        _description_
    content_type: _type_
        _description_
    content_name: _type_
        _description_
   content_url: _type_
        _description_

    Returns
    -------
    potential_a11y_issues: list[dict[str,str|int]]
        List of dictionaries, with each dictionary corresponding to a single
        item in the course site that may have accessibility issues.

    Raises
    ------
    ValueError
        _description_
    '''

    potential_a11y_issues = []

    if not html_string:
        logger.debug("html_string is empty. Returning empty list...")
        return potential_a11y_issues
    else:
        logger.debug("html string not empty. Extracting now...")
    soup = BeautifulSoup(html_string, 'html.parser')
    run_id = int(datetime.datetime.now().timestamp())

    for a_tag in soup.find_all('a', href=True):
        link_url = a_tag['href']
        link_text = a_tag.get_text(strip=True)
        potential_a11y_issues.append({
            "course_id": course_id,
            "audit_status": "New Content",
            "content-type": content_type,
            "display_name": content_name,
            "url": content_url,
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
        Dictionary of configuration settings from config.toml
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
    logger.debug(canvas_df.info())
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
        Set of columns to include, by default includes all columns.
    dtypebackend: str
        The dtype backend you want for your dataframe ("pyarrow", by default)

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
        logger.debug(ally_df.info())
        return ally_df
    logger.debug(f"shape of ally_df = {ally_df.shape}")
    logger.debug(f"ally_df info : {ally_df.info()}")
    return ally_df.loc[ally_df["Deleted at"].isna()]


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
def create_csv(df: DataFrame) -> None:
    df.loc[df["Score"].notna(),"Score"] = df.loc[df["Score"].notna(),"Score"] * 100
    return df.to_csv(
        "accessibility_review.csv",
        na_rep="",
        mode="x",
        float_format="%.2f",
    )

# TODO Use conditional to skip parse-extract func w/files.
@logger.catch()
def main(config_path: Path = config_file) -> str:
    """Orchestrate the retrieval and combining of Ally and Canvas data."""
    logger.info("Starting program...")
    run_id = int(datetime.datetime.now().timestamp())
    config = load_config(config_path)
    course_id = config.get("course_id")

    course_obj = initialize_canvas_course(config)

    course_content_dict = fetch_course_content(course_obj, config)

    course_files = course_content_dict.get("Files")
    course_file_data = parse_course_file_data(course_files,course_id)
#TODO Start here...
    potential_a11y_issues = []
    for content_type,course_content in course_content_dict.items():
        if content_type != "Files":
            html_string,content_name = extract_html(course_content,content_type,config)
            content_type_a11y_issues = parse_html_content(html_string,course_id,content_type,content_name,"URL_PLACEHOLDER")
            potential_a11y_issues.extend(content_type_a11y_issues)
    canvas_df = create_canvas_data_df(config,course_file_data,potential_a11y_issues)
    ally_df = create_ally_df(
        "/Users/harkmorper/Downloads/ally-31318-OPPrimary_-_SOCWRK_526_-_The_Evaluation_and_Treatment_of_Mental_Disorders_-2026-01-12-15-11.csv",
    )
    drop_joint_columns = config.get("joint").get("drop_columns")
    joint_df = join_data_sources(canvas_df, ally_df, drop_joint_columns)
    create_csv(joint_df)
    success_message = "Congratulations! DataFrame written to CSV!"
    logger.success(success_message)
    return success_message


# =============================================================================
# EXECUTION - Orchestrator
# =============================================================================
if __name__ == "__main__":
    success_message = main()
    print(success_message)