# To-Do List

- Docs: README
- Feat: Add ability to run a list of courses
- Feat: Create command line interface
- Fix: Double check that the scraping is correctly pulling all the data I want it to correctly.
  - Test: Write some tests for this probably
  - Fix: File Scraper needs to identify the type of file (by mime-type I think)
  - HTML Scraper:
    - Feat: Look through audit example and consider matching level of complexity/depth
    - Fix: Figure out how to get the urls in there.
- Refactor/Perf: graphql interface?
- Feat: Add pydantic data validation
- Refactor/Build/CI: Make storing of tokens more secure
- Chore: Clean up logging outputs.
- Feat: Change output name to accessibility*review*{course number}
  - Feat: place in: path / "{department*prefix}" / "{course name}*{course_number}"
- Build: Update pyproject.toml
