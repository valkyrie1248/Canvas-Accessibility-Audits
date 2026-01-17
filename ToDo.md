# To-Do List

- Chore: README
- Chore: Tidy up docstrings
- Feat: Add ability to run a list of courses
- Feat: Create command line interface
- Bug: Double check that the scraping is correctly pulling all the data I want it to correctly.
  - Chore: Write some tests for this probably
  - Bug: File Scraper needs to identify the type of file (by mime-type I think)
  - HTML Scraper:
    - Feat: Look through audit example and consider matching level of complexity/depth
    - Bug: Figure out how to get the urls in there.
- Refactor: graphql interface?
- Refactor: Only define course_id and run_id once (in main), so you don't end up with conflicts.
- Feat: Add pydantic data validation
- Make storing of tokens more secure
- Chore: Clean up logging outputs.
