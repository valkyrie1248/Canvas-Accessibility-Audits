import marimo

__generated_with = "0.19.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import datetime
    import time
    import json
    import sys
    import requests  # Consider aiohttp if I want to do this async for performance reasons
    # import jwt

    # from requests_oauthlib import OAuth1
    from loguru import logger
    from canvasapi import Canvas
    import marimo as mo
    import pandas as pd
    import numpy as np
    return Canvas, logger, mo, pd, requests, sys


@app.cell
def _(logger, pd, sys):
    # =============================================================================
    # CONFIGURATION
    # =============================================================================

    # =============================================================================
    # CONFIGURATION - General/Display
    # =============================================================================

    pd.options.display.float_format = lambda x: "%.3f" % x
    pd.options.display.max_rows = 500
    pd.options.display.max_columns = 100
    pd.set_option("display.precision", 4)


    # =============================================================================
    # CONFIGURATION - Logger
    # =============================================================================
    logger.remove()
    logger.add(
        sys.stderr,
        colorize=True,
        format="<green>{time:YYYY-MM-DD at HH:mm:ss}</green>| <level>{level}</level> | <level>{message}</level> | {extra}",
        backtrace=True,
        diagnose=True,
        level="TRACE",
    )
    logger.add(
        "expense_pipeline.log",
        colorize=True,
        rotation="1 week",
        retention=5,
        format="<green>{time:YYYY-MM-DD at HH:mm:ss}</green>| <level>{level}</level> | <level>{message}</level> | {extra}",
        backtrace=True,
        diagnose=True,
        level="TRACE",
    )
    return


@app.cell
def _(mo):
    # =============================================================================
    # CONSTANTS - API Configuration
    # =============================================================================

    canvas_url = "https://boisestatecanvas.instructure.com/"
    canvas_token = "15177~HmTELDkkyAQTXctakDmUt9B3MKYKeWKYJVE36zumEJJZ6u8th9TKPwA3WBTMJ9F2"
    canvas_columns = [
            "id",
            "folder_id",
            "folder_id_date",
            "display_name",
            "filename",
            "upload_status",
            "content-type",
            "url",
            "size",
            "created_at",
            "created_at_date",
            "updated_at",
            "updated_at_date",
            "unlock_at",
            "locked",
            "hidden",
            "lock_at",
            "hidden_for_user",
            "thumbnail_url",
            "modified_at",
            "modified_at_date",
            "mime_class",
            "media_entry_id",
            "category",
            "locked_for_user",
            "visibility_level",
            "size_date",
        ]
    drop_canvas_columns = ["_requester"]


    ally_region = "prod.ally.ac"
    ally_client_id = "11637"
    ally_bearer_token = mo.ui.text(
        value="eyJhbGciOiJIUzI1NiJ9.eyJ1c2VyIjp7InJvbGUiOiJBbGx5Q291cnNlUmVwb3J0QXBpVXNlciIsIm5hbWUiOiI8Pjw-IiwiY2xpZW50SWQiOjExNjM3LCJ1c2VySWQiOiItMSJ9LCJleHAiOjE3OTk2NDQ5NjMsImlhdCI6MTc2ODEwODk2M30.ZhwWla72CNEvLg8v_1DwyD1XlvMVaC-xuFss9H-cgNM",
        kind="password",
    )
    ally_key = mo.ui.text(
        value="HA41OiNhgkYSDAqHohaF4ZVCzeeywf0o", kind="password"
    )
    ally_secret = mo.ui.text(
        value="QNq1PwOFUbww0omh3IgTIwJcU00gEKHg".strip(), kind="password"
    )
    course_id = "31318"
    return (
        ally_bearer_token,
        ally_client_id,
        ally_region,
        canvas_token,
        canvas_url,
        course_id,
        drop_canvas_columns,
    )


@app.cell
def constants_ally_initialization(
    ally_bearer_token,
    ally_client_id,
    ally_region,
    course_id,
    requests,
):
    # =============================================================================
    # CONSTANTS - Ally Initialization
    # =============================================================================
    ally_issues_endpoint = f"https://{ally_region}/api/v2/clients/{ally_client_id}/reports/issues"
    ally_overall_endpoint = f"https://{ally_region}/api/v2/clients/{ally_client_id}/reports/overall"
    ally_v2_content_endpoint = f"https://{ally_region}/api/v2/clients/{ally_client_id}/courses/{course_id}/content"

    # ally_token = generate_ally_token()
    ally_token = ally_bearer_token.value
    # auth = OAuth1(
    #     client_key=ally_key.value, 
    #     client_secret=ally_secret.value, 
    #     signature_method='HMAC-SHA1'
    # )


    headers = {
        "Authorization": f"Bearer {ally_token}",
        "Accept": "application/json",
        "User-Agent": "AllyChecklistMVP/1.0 (Contact: jeremyharper221@boisestate.edu)",  # Adding a custom UA
    }
    parameters ={
        "courseId": course_id
    }

    parameters_csv = {
        "role": "urn:lti:role:ims/lis/Administrator",
        "userId": "1"
    }

    response = requests.get(ally_v2_content_endpoint,headers=headers)
    response
    return


@app.cell
def _():
    # # =============================================================================
    # # INITIALIZATION - Ally JWT Generation
    # # =============================================================================

    # token = jwt.encode(payload, secret, algorithm="HS256")
    return


@app.cell
def _(Canvas, drop_canvas_columns, logger, pd):
    # =============================================================================
    # FUNCTIONS
    # =============================================================================
    # def load_config(config_path=config_file) -> dict:
    #     """Parse toml file containing configuration and data schema rules.

    #     Return:
    #     Dictionary containing all rules, equivalencies, and configuration settings.

    #     """
    #     try:
    #         with Path(config_path).open("rb") as f:
    #             return tomllib.load(f)
    #     except FileNotFoundError:
    #         logger.exception(f"Config file {config_path} not found.")
    #         return {}


    # @logger.catch
    # def generate_ally_token(client_id: str = ally_client_id, client_secret: str = ally_secret.value):
    #     """
    #     Generates a JWT for Ally API authentication.

    #     Args:
    #         client_id (str): Your Ally Client ID (iss).
    #         client_secret (str): Your Ally Client Secret (signing key).

    #     Returns:
    #         str: The encoded JWT string ready for the Authorization header.
    #     """


    #     now_utc = datetime.datetime.now(datetime.timezone.utc)

    #     payload = {
    #         "user": {
    #             "role": "administrator", # Required for V2 reports
    #             "name": "ScriptUser",
    #             "clientId": int(client_id),
    #             "userId": "-1" # -1 is standard for system-level access
    #         },
    #         "iat": int(now_utc.timestamp()),
    #         "exp": int((now_utc + datetime.timedelta(hours=1)).timestamp()),
    #     }

    #     token = jwt.encode(payload, client_secret, algorithm="HS256")
    #     return token

    @logger.catch(message="Failed to fetch Canvas course content. See traceback for details.")
    def fetch_canvas_course(
        canvas_url:str, 
        canvas_token:str, 
        course_id:int,drop_cols:list[str]=["_requester"],
        dtypebackend:str="pyarrow"
    ):
        '''Download canvas course info and load it into a Pandas DataFrame

                Parameters
                ----------
                canvas_url: str
                    The base url for your institution's canvas site.
                canvas_token: str
                    Access token generated by the user from within the canvas user settings.
                course_id : int
                    ID number associated with the course site you are auditing.
                drop_cols: list[str], optional
                    List of columns you don't need (["_requester"], by default)
                dtypebackend: str, optional
                    The dtype backend you want for your dataframe ("pyarrow", by default)

                Raises
                ------
                None for now
                    None for now

                Returns
                -------
                canvas_df: DataFrame
                    A pandas DataFrame
            '''
        logger.info("Initializing Canvas object...")
        canvas = Canvas(canvas_url, canvas_token)
        logger.info("Fetching course site data...")
        course = canvas.get_course(course_id)
        logger.info("Fetching course files data...")
        course_files = course.get_files()

        logger.info("Initializing DataFrame")
        canvas_df = (
            pd.DataFrame([file.__dict__ for file in course_files])
                .drop(columns=drop_canvas_columns)
                .convert_dtypes(dtype_backend=dtypebackend)
        )
        return canvas_df

    @logger.catch(message="Failed to load ally DataFrame. See traceback for details.")
    def create_ally_df(file_path: str, dtypebackend: str = "pyarrow", use_columns: list[str] | None=None, show_deleted:bool = False):
        '''Create a Pandas DataFrame from a downloaded Ally course report.

                Parameters
                ----------
                file_path: Path | str
                    The full or relative path of the downloaded course report.
                use_columns: list[str], optional
                    Set of columns to include, by default includes all columns.
                dtypebackend: str
                    The dtype backend you want for your dataframe ("pyarrow", by default)

                Raises
                ------
                None for now
                    None for now

                Returns
                -------
                ally_df: DataFrame
                    A pandas DataFrame
        '''
        try:
            ally_df = pd.read_csv(
                file_path,
                dtype_backend=dtypebackend,
                usecols=use_columns,
                engine="pyarrow"
            )
        except Exception as e:
            logger.critical(f"Pandas load failed: {e}")
            raise
        # ally_df[["Item type","id"]]=ally_df["Id"].str.split(":",expand=True)
        # ally_df=ally_df.set_index("id")
        if show_deleted == True:
            return ally_df
        if show_deleted == False:
            return ally_df.loc[ally_df["Deleted at"].isna()]

    @logger.catch(message="Failed to join DataFrames. See traceback for details.")
    def join_data_sources(canvas_df, ally_df):
        '''Join Ally and Canvas DataFrames on their corresponding file name columns.

                Parameters
                ----------
                canvas_df: DataFrame
                    DataFrame of all files from Canvas course site. (Could also be all content from canvas if you create it right.)
                ally_df: DataFrame
                    DataFrame of all content in Canvas course site that Ally has checked for accessibility.

                Raises
                ------
                None for now
                    None for now

                Returns
                -------
                joint_df: DataFrame
                    A pandas DataFrame that combines the data from Canvas and Ally.
        '''
        joint_df = pd.merge(canvas_df.copy(),ally_df.copy(),left_on="display_name",right_on="Name",how="outer")
        joint_df = joint_df.loc[joint_df["Score"].notna()]
        return joint_df
    return create_ally_df, fetch_canvas_course


@app.cell
def _(canvas_token, canvas_url, course_id, fetch_canvas_course):
    # =============================================================================
    # CONSTANTS - Canvas Initialization
    # =============================================================================
    canvas_df = fetch_canvas_course(canvas_url, canvas_token,course_id)
    return (canvas_df,)


@app.cell
def _(Canvas, canvas_df, canvas_token, canvas_url, course_id):
    canvas = Canvas(canvas_url, canvas_token)
    course = canvas.get_course(course_id)
    for col in canvas_df.columns:
        print(col)
    print(len(canvas_df.columns))
    files= course.get_files()
    for f in files[0].__dict__:
        print(f)
    len(files[0].__dict__)
    return (course,)


@app.cell
def _(course):
    pages = course.get_pages()
    for page in pages:
        print(page.__dict__)
    for item in pages[0].__dict__:
        print(item)
    pages[0].__dict__.get("hide_from_students")
    return


@app.cell
def _(mo):
    mo.md(r"""
    for fi in files[0].__dict__:
        print(fi)
    print("============================")
    for p in pages[0].__dict__:
        print(p)
    """)
    return


@app.cell
def _(ally_df, canvas_df):
    # canvas_df_pdf = canvas_df.loc[canvas_df["content-type"].str.contains("pdf")]
    # ally_df_pdf = ally_df.loc[ally_df["Mime type"].str.contains("pdf")]
    # for l_ally in list(ally_df_pdf["Name"].sort_values()):
    #     if l_ally not in list(canvas_df_pdf["display_name"].sort_values()):
    #         print(l_ally)
    # for l_canvas in list(canvas_df_pdf["display_name"].sort_values()):
    #     if l_canvas not in list(ally_df_pdf["Name"].sort_values()):
    #         print(l_canvas)

    canvas_df_xml = canvas_df.loc[canvas_df["content-type"].str.contains("xml")]
    ally_df_xml = ally_df.loc[ally_df["Mime type"].str.contains("xml")]
    for l_ally in list(ally_df_xml["Name"].sort_values()):
        if l_ally not in list(canvas_df_xml["display_name"].sort_values()):
            print(l_ally)
    for l_canvas in list(canvas_df_xml["display_name"].sort_values()):
        if l_canvas not in list(ally_df_xml["Name"].sort_values()):
            print(l_canvas)
    return


@app.cell
def _(create_ally_df):
    ally_df = create_ally_df("/Users/harkmorper/Downloads/ally-31318-OPPrimary_-_SOCWRK_526_-_The_Evaluation_and_Treatment_of_Mental_Disorders_-2026-01-12-15-11.csv")
    ally_df = ally_df.rename(columns={"Name": "display_name"})
    ally_df
    return (ally_df,)


if __name__ == "__main__":
    app.run()
