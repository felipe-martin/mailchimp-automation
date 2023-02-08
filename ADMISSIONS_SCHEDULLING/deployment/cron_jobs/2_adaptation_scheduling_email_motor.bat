@echo off
echo "[INFO] /////////////// PROCESSING CHILD SCHEDULED ADAPTATIONS TO SEND ADAPTATION SURVEY BY EMAIL //////////////"
timeout 1 > NUL
call C:\Users\LMS\Anaconda3\Scripts\activate mailchimp
timeout 5 > NUL
cd C:\Users\LMS\Documents\MAILCHIMP_AUTOMATION\ADMISSIONS_SCHEDULLING\deployment\
timeout 1 > NUL
python adaptation_scheduling_email_motor.py --campaing ENADAP 