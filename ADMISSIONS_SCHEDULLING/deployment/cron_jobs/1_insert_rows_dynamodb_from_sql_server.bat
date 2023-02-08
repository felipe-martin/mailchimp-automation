@echo off
echo "[INFO] /////////////// INSERTING ROWS FROM SQL SERVER VITAMINA TO AWS DYNAMODB //////////////"
timeout 1 > NUL
call C:\Users\LMS\Anaconda3\Scripts\activate mailchimp
timeout 5 > NUL
cd C:\Users\LMS\Documents\MAILCHIMP_AUTOMATION\ADMISSIONS_SCHEDULLING\deployment\
timeout 1 > NUL
python insert_rows_dynamodb_from_sql_server.py