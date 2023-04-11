@echo off
echo "[INFO] /////////////// PROCESSING DATABASE TO FEED QUIKSIGHT MONITORING //////////////"
timeout 1 > NUL
call C:\Users\LMS\Anaconda3\Scripts\activate mailchimp
timeout 5 > NUL
cd C:\Users\LMS\Documents\MAILCHIMP_AUTOMATION\ADMISSIONS_SCHEDULLING\deployment\utils\
timeout 1 > NUL
python create_quiksight_file.py