# To-Do List

- README
- Tidy up docstrings
- Add ability to run a list of courses
- Create command line interface
- Pull data beyond files from canvas api
  - Port the audit_html_content function from audit 3.0
    - Update it so it doesn't exclude internal links
  - Figure out how to clean the html audit data so its df can merge nicely
- Refactor with graphql interface
- Refactor: Only define course_id and run_id once (in main), so you don't end up with conflicts.
