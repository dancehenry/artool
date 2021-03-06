# artool

## Installation Instruction 

1. download the latest version (14.2) PostgreSQL database from this link: [PostgreSQL](https://sbp.enterprisedb.com/getfile.jsp?fileid=1257974) (*Click to download*)
2. install it and please make sure to keep a note of [<ins>the password for the database superuser (postgres)</ins>](/instruction/installation/password_screenshot.png) (*Click for screenshot*), and <ins>the port number (e.g. 5432 by default)</ins>.
3. download the zipped file, "artool.zip" and unzip it.
4. you are ready to go, please follow the "Execution Instruction" below

## Execution Instruction

1. Please double click the file "artool.exe". If there is anything wrong, please right click the "artool.exe" and run as administrator.
2. Once the window pops up, please fill in the blanks. Here is an [<ins>example</ins>](/instruction/execution/artool_form_example.png) (*Click for screenshot*).
3. Click the button "DB Connect" first.
4. Once you see a green check mark next to "DB Connect", then click the button "Execute".

## Version History

### version 3.2
1. Added query_template.sql in bin folder for urgency situations to apply hot fixes.
2. If the query_template.sql is not available, it will fall back and utilize the built-in query.
3. Output query executed in output folder after each execution.

### version 3.1
1. Fixed a bug that previous month data removed from final output.

### version 3.0
1. Bundled the JRE and JAR into exe file.
2. Designed an ico.
3. Added alerts for errors and success info.
4. Added payment comparison.

### version 2.2 
1. Added the customer payment comparison among customer_payment, output, and the unmatched.
2. Added comments in the query.

### version 2.1 
1. Add user_profile.json to remember user's profile and automatically fill in on the next run.
2. Remove the print out query.
