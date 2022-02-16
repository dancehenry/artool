# artool

## ==== Installation Instruction ======================

1. download the latest version (14.2) PostgreSQL database from this link: [PostgreSQL](https://sbp.enterprisedb.com/getfile.jsp?fileid=1257974) (*Click to download*)
2. install it and please make sure to keep a note of [<ins>the password for the database superuser (postgres)</ins>](/installation/password_screenshot.png) (*Click for screenshot*), and <ins>the port number (e.g. 5432 by default)</ins>.
3. download the latest version (17.0.2) Java SE Runtime Environment from this link: [JRE 17.0.2](https://download.oracle.com/java/17/latest/jdk-17_windows-x64_bin.exe) (*Click to download*)
4. install it and configure the system environment variable.
   - Open the Control Panel.
   - Navigate to the following applet: Control Panel\System and Security\System\Advanced System Settings.
   - Click on the button "Environment Variables...".
   - [Edit system variable "Path"](/installation/SystemVariable_Update_Path.png) (*Click for screenshot*).
   - [Add java to the Path](/installation/SystemVariable_Add_Java.png) (*Click for screenshot*)

## ==== Execution Instruction ======================

1. Please double click the file "run" or "run.bat" file.
2. Once the window pops up, please fill in the blanks.
3. Click the button "DB Connect" first.
4. Once you see a green check mark next to "DB Connect", then click the button "Execute".

## ==== Version History ===========================

### version 2.2 
1. Added the customer payment comparison among customer_payment, output, and the unmatched.
2. Added comments in the query.

### version 2.1 
1. Add user_profile.json to remember user's profile and automatically fill in on the next run.
2. Remove the print out query.
