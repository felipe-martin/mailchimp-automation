@echo off
echo "[INFO] /////////////// PROCESSING TO MARK NOT CHILD SCHEDULED ADAPTATIONS //////////////"
timeout 1 > NUL
call C:\Users\LMS\Anaconda3\Scripts\activate mailchimp
timeout 5 > NUL
cd C:\Users\LMS\Documents\MAILCHIMP_AUTOMATION\ADMISSIONS_SCHEDULLING\deployment_with_xano\utils\
timeout 1 > NUL
python mark_unscheduled_process.py